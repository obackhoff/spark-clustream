package com.backhoff.clustream

/**
  * Created by omar on 9/25/15.
  */

import breeze.linalg._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}


@Experimental
class CluStreamOnline(
                      val q: Int,
                      val alpha: Int,
                      val alphaModifier: Int,
                      val numDimensions: Int,
                      val minInitPoints: Int)
  extends Logging with Serializable {

  def timer[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  private val mLastPoints: Double = 300.0
  private val delta = 4
  private val tFactor = 2

  private var time: Long = 0L
  private var N: Long = 0L

  private var microClusters: Array[MicroCluster] = null
  private var mcInfo: Array[(MicroClusterInfo, Int)] = null

  private var broadcastQ: Broadcast[Int] = null
  private var broadcastMCInfo: Broadcast[Array[(MicroClusterInfo, Int)]] = null

  var initialized = false
  private var initArr: Array[breeze.linalg.Vector[Double]] = Array()

  private def initRand(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {
    microClusters = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0.0), Vector.fill[Double](numDimensions)(0.0), 0L, 0L, 0L))
    mcInfo = Array.fill(q)(new MicroClusterInfo(Vector.fill[Double](numDimensions)(rand()), 0.0, 0L)) zip (0 until q)

    val assignations = assignToMicroCluster(rdd, q, mcInfo)
    updateMicroClusters(assignations)
    var i = 0
    for (mc <- microClusters) {
      mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
      if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
      mcInfo(i)._1.setN(mc.getN)
      if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n)))
      else {
        mcInfo(i)._1.setRmsd(distanceNearestMC(mcInfo(i)._1.centroid, mcInfo))
        //        println("YEAH, 1-SIZE MC, rmsd = " + mcInfo(i)._1.rmsd)
      }
      i += 1
    }

    broadcastQ = rdd.context.broadcast(q)
    broadcastMCInfo = rdd.context.broadcast(mcInfo)
    initialized = true
  }

  private def initKmeans(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {
    initArr = initArr ++ rdd.collect
    if (initArr.length >= minInitPoints) {

      import org.apache.spark.mllib.clustering.KMeans
      val trainingSet = rdd.context.parallelize(initArr.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray)))
      val clusters = KMeans.train(trainingSet, q, 10)
      trainingSet.unpersist(blocking = false)

      mcInfo = Array.fill(q)(new MicroClusterInfo(Vector.fill[Double](numDimensions)(0), 0.0, 0L)) zip (0 until q)
      for (i <- clusters.clusterCenters.indices) mcInfo(i)._1.setCentroid(DenseVector(clusters.clusterCenters(i).toArray))

      microClusters = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0), Vector.fill[Double](numDimensions)(0), 0L, 0L, 0L))
      val tempRDD = rdd.context.parallelize(initArr)
      val assignations = assignToMicroCluster(tempRDD, q, mcInfo)
      tempRDD.unpersist(blocking = false)
      updateMicroClusters(assignations)

      var i = 0
      for (mc <- microClusters) {
        mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
        if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
        mcInfo(i)._1.setN(mc.getN)
        if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n)))
        else mcInfo(i)._1.setRmsd(distanceNearestMC(mcInfo(i)._1.centroid, mcInfo))
        i += 1
      }

      broadcastQ = rdd.context.broadcast(q)
      broadcastMCInfo = rdd.context.broadcast(mcInfo)

      initialized = true
    }
  }

  def run(data: DStream[breeze.linalg.Vector[Double]]): Unit = {
    data.foreachRDD { (rdd, time) =>
      this.time += 1
      this.N += rdd.count()
      if (!rdd.isEmpty()) {

        if (initialized) {

          val assignations = assignToMicroCluster(rdd, broadcastQ.value, broadcastMCInfo.value)
          updateMicroClusters(assignations)
          var i = 0
          for (mc <- microClusters) {
            mcInfo(i) = (mcInfo(i)._1, mc.getIds(0))
            if (mc.getN > 0) mcInfo(i)._1.setCentroid(mc.cf1x :/ mc.n.toDouble)
            mcInfo(i)._1.setN(mc.getN)
            if (mcInfo(i)._1.n > 1) mcInfo(i)._1.setRmsd(scala.math.sqrt(sum(mc.cf2x) / mc.n - sum(mc.cf1x.map(a => a * a)) / (mc.n * mc.n)))
            else {
              mcInfo(i)._1.setRmsd(distanceNearestMC(mcInfo(i)._1.centroid, broadcastMCInfo.value))
              //            println("YEAH, 1-SIZE MC, rmsd = " + mcInfo(i)._1.rmsd)
            }
            i += 1
          }

          broadcastMCInfo = rdd.context.broadcast(mcInfo)

          //PRINT STUFF FOR DEBUGING

          microClusters.foreach { mc =>
            //            println("IDs " + mc.getIds.mkString(" "))
            //            println("CF1X: " + mc.getCf1x.toString)
            //            println("CF2X: " + mc.getCf2x.toString)
            //            println("CF1T: " + mc.getCf1t.toString)
            //            println("CF2T: " + mc.getCf2t.toString)
            //            println("N: " + mc.getN.toString)
            //            println()
          }
//          println("Centers: ")
//          broadcastMCInfo.value.foreach(a => println("Cluster " + a._2 + "=" + a._1.centroid))
          //          println("RMSD: ")
          //          broadcastMCInfo.value.foreach(a => println("Cluster " + a._2 + "=" + a._1.rmsd))
          //          println("Total time units elapsed: " + this.time)
//          println("Total number of points: " + N)
//          println("N alternativo: ")
//          broadcastMCInfo.value.foreach(a => println("Cluster " + a._2 + "=" + a._1.n))

        } else {
          minInitPoints match {
            case 0 => initRand(rdd)
            case _ => initKmeans(rdd)
          }
        }

      }
    }
  }

  def getMicroClusters(): Array[MicroCluster] = {
    this.microClusters
  }

  private def distanceNearestMC(vec: breeze.linalg.Vector[Double], mcs: Array[(MicroClusterInfo, Int)]): Double = {

    var minDist = Double.PositiveInfinity
    var i = 0
    for (mc <- mcs) {
      val dist = squaredDistance(vec, mc._1.centroid)
      if (dist != 0.0 && dist < minDist) minDist = dist
      i += 1
    }
    scala.math.sqrt(minDist)
  }

  private def squaredDistTwoMCArrIdx(idx1: Int, idx2: Int): Double = {
    squaredDistance(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble, microClusters(idx2).getCf1x :/ microClusters(idx2).getN.toDouble)
  }

  private def squaredDistPointToMCArrIdx(idx1: Int, point: Vector[Double]): Double = {
    squaredDistance(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble, point)
  }

  private def getArrIdxMC(idx0: Int): Int = {
    var id = -1
    var i = 0
    for (mc <- microClusters) {
      if (mc.getIds(0) == idx0) id = i
      i += 1
    }
    id
  }

  private def saveSnapshot(): Unit = {}

  private def mergeMicroClusters(idx1: Int, idx2: Int): Unit = {

    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x :+ microClusters(idx2).getCf1x)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x :+ microClusters(idx2).getCf2x)
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + microClusters(idx2).getCf1t)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + microClusters(idx2).getCf2t)
    microClusters(idx1).setN(microClusters(idx1).getN + microClusters(idx2).getN)
    microClusters(idx1).setIds(microClusters(idx1).getIds ++ microClusters(idx2).getIds)

    mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble)
    mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n)))

  }

  private def addPointMicroClusters(idx1: Int, point: Vector[Double]): Unit = {

    microClusters(idx1).setCf1x(microClusters(idx1).getCf1x :+ point)
    microClusters(idx1).setCf2x(microClusters(idx1).getCf2x :+ (point :* point))
    microClusters(idx1).setCf1t(microClusters(idx1).getCf1t + this.time)
    microClusters(idx1).setCf2t(microClusters(idx1).getCf2t + (this.time * this.time))
    microClusters(idx1).setN(microClusters(idx1).getN + 1)

    mcInfo(idx1)._1.setCentroid(microClusters(idx1).getCf1x :/ microClusters(idx1).getN.toDouble)
    mcInfo(idx1)._1.setN(microClusters(idx1).getN)
    mcInfo(idx1)._1.setRmsd(scala.math.sqrt(sum(microClusters(idx1).cf2x) / microClusters(idx1).n - sum(microClusters(idx1).cf1x.map(a => a * a)) / (microClusters(idx1).n * microClusters(idx1).n)))

  }

  private def replaceMicroCluster(idx: Int, point: Vector[Double]): Unit = {
    microClusters(idx) = new MicroCluster(point :* point, point, this.time * this.time, this.time, 1L)
    mcInfo(idx)._1.setCentroid(point)
    mcInfo(idx)._1.setN(1L)
    mcInfo(idx)._1.setRmsd(distanceNearestMC(mcInfo(idx)._1.centroid, mcInfo))
  }

  private def assignToMicroCluster(rdd: RDD[Vector[Double]], q: Int, mcInfo: Array[(MicroClusterInfo, Int)]): RDD[(Int, Vector[Double])] = {
    rdd.map { a =>
      //val arr = Array.fill[(Int, Double)](q)(0, 0)
      var minDist = Double.PositiveInfinity
      var minIndex = Int.MaxValue
      var i = 0
      for (mc <- mcInfo) {
        //arr(i) = (mc._2, squaredDistance(a, mc._1.centroid))
        val dist = squaredDistance(a, mc._1.centroid)
        if (dist < minDist) {
          minDist = dist
          minIndex = mc._2
        }
        i += 1
      }
      (minIndex, a)
    }
  }

  private def updateMicroClusters(assignations: RDD[(Int, Vector[Double])]): Unit = {

    var dataInAndOut: RDD[(Int, (Int, Vector[Double]))] = null
    var dataIn: RDD[(Int, Vector[Double])] = null
    var dataOut: RDD[(Int, Vector[Double])] = null

    // cache() plays an important role on performance :)
    println("compare RMSD")
    timer {
      if (initialized) {
        dataInAndOut = assignations.map { a =>
          val nearMCInfo = broadcastMCInfo.value.find(id => id._2 == a._1).get._1
          val nearDistance = scala.math.sqrt(squaredDistance(a._2, nearMCInfo.centroid))

          if (nearDistance <= tFactor * nearMCInfo.rmsd) (1, a)
          else (0, a)
        }
      }
    }
    println("separate data")
    timer {
      if (dataInAndOut != null) {
        dataIn = dataInAndOut.filter(_._1 == 1).map(a => a._2).cache()
        dataOut = dataInAndOut.filter(_._1 == 0).map(a => a._2).cache()
        dataInAndOut.unpersist(blocking = false)
        assignations.unpersist(blocking = false)
      } else dataIn = assignations
    }
    println("calc info")
    val pointCount = timer {
      dataIn.countByKey()
    }
    val sums = timer {
      dataIn.reduceByKey(_ :+ _).collect()
    }
    val sumsSquares = timer {
      dataIn.mapValues(a => a :* a).reduceByKey(_ :+ _).collect()
    }


    println("update microClusters")
    timer {
      for (mc <- microClusters) {
        for (s <- sums) if (mc.getIds(0) == s._1) mc.setCf1x(mc.cf1x :+ s._2)
        for (ss <- sumsSquares) if (mc.getIds(0) == ss._1) mc.setCf2x(mc.cf2x :+ ss._2)
        for (pc <- pointCount) if (mc.getIds(0) == pc._1) {
          mc.setN(mc.n + pc._2)
          mc.setCf1t(mc.cf1t + pc._2 * this.time)
          mc.setCf2t(mc.cf2t + pc._2 * (this.time * this.time))
        }
      }
    }

    println("Deal with outliers")
    timer {
      if (dataOut != null) {
        var mTimeStamp: Double = 0.0
        val recencyThreshhold = this.time - delta
        var safeDeleteMC: Array[Int] = Array()
        var keepOrMergeMC: Array[Int] = Array()
        var i = 0

        for (mc <- microClusters) {
          val meanTimeStamp = if (mc.getN > 0) mc.getCf1t.toDouble / mc.getN.toDouble else 0
          val sdTimeStamp = scala.math.sqrt(mc.getCf2t.toDouble / mc.getN.toDouble - meanTimeStamp * meanTimeStamp)

          if (mc.getN < tFactor * mLastPoints) mTimeStamp = meanTimeStamp
          else mTimeStamp = breeze.stats.distributions.Gaussian(meanTimeStamp, sdTimeStamp).icdf(1 - mLastPoints / (2 * mc.getN.toDouble))

          if (mTimeStamp < recencyThreshhold) safeDeleteMC = safeDeleteMC :+ i
          else keepOrMergeMC = keepOrMergeMC :+ i

          i += 1
        }

        var j = 0
        var newMC: Array[Int] = Array()

        for (point <- dataOut.collect()) {

          var minDist = Double.PositiveInfinity
          var idMinDist = 0
          for(id <- newMC){
            val dist = squaredDistPointToMCArrIdx(id, point._2)
            if (dist < minDist) {
              minDist = dist
              idMinDist = id
            }
          }

          var rmsd = 0.0
          if (microClusters(idMinDist).getN > 1)
            rmsd = scala.math.sqrt(sum(microClusters(idMinDist).cf2x) / microClusters(idMinDist).n - sum(microClusters(idMinDist).cf1x.map(a => a * a)) / (microClusters(idMinDist).n * microClusters(idMinDist).n))
          else rmsd = distanceNearestMC(mcInfo(idMinDist)._1.centroid, broadcastMCInfo.value)

          if (minDist <= 2 * rmsd) addPointMicroClusters(idMinDist, point._2)
          else if (safeDeleteMC.lift(j).isDefined) {
            replaceMicroCluster(safeDeleteMC(j), point._2)
            newMC = newMC :+ safeDeleteMC(j)
            j += 1
          } else {
            var minDist = Double.PositiveInfinity
            var idx1 = 0
            var idx2 = 0
            for (a <- keepOrMergeMC.indices)
              for (b <- (0 + a) until keepOrMergeMC.length) {
                var dist = Double.PositiveInfinity
                if (keepOrMergeMC(a) != keepOrMergeMC(b)) dist = squaredDistTwoMCArrIdx(keepOrMergeMC(a), keepOrMergeMC(b))
                if (dist < minDist) {
                  minDist = dist
                  idx1 = keepOrMergeMC(a)
                  idx2 = keepOrMergeMC(b)
                }
              }
            mergeMicroClusters(idx1, idx2)
            replaceMicroCluster(idx2, point._2)
            newMC = newMC :+ idx2
          }
        }
      }
      dataIn.unpersist(blocking = false)

    }
  }

  // END OF MODEL
}

private object MicroCluster extends Serializable {
  private var current = -1

  private def inc = {
    current += 1
    current
  }
}

protected class MicroCluster(
                            var cf2x: breeze.linalg.Vector[Double],
                            var cf1x: breeze.linalg.Vector[Double],
                            var cf2t: Long,
                            var cf1t: Long,
                            var n: Long,
                            var ids: Array[Int]) extends Serializable {

  def this(cf2x: breeze.linalg.Vector[Double], cf1x: breeze.linalg.Vector[Double], cf2t: Long, cf1t: Long, n: Long) = this(cf2x, cf1x, cf2t, cf2t, n, Array(MicroCluster.inc))

  def setCf2x(cf2x: breeze.linalg.Vector[Double]): Unit = {
    this.cf2x = cf2x
  }

  def getCf2x: breeze.linalg.Vector[Double] = {
    this.cf2x
  }

  def setCf1x(cf1x: breeze.linalg.Vector[Double]): Unit = {
    this.cf1x = cf1x
  }

  def getCf1x: breeze.linalg.Vector[Double] = {
    this.cf1x
  }

  def setCf2t(cf2t: Long): Unit = {
    this.cf2t = cf2t
  }

  def getCf2t: Long = {
    this.cf2t
  }

  def setCf1t(cf1t: Long): Unit = {
    this.cf1t = cf1t
  }

  def getCf1t: Long = {
    this.cf1t
  }

  def setN(n: Long): Unit = {
    this.n = n
  }

  def getN: Long = {
    this.n
  }

  def setIds(ids: Array[Int]): Unit = {
    this.ids = ids
  }

  def getIds: Array[Int] = {
    this.ids
  }
}

private class OrderingDoubleTuple extends Ordering[Tuple2[Int, Double]] with Serializable {
  override def compare(x: (Int, Double), y: (Int, Double)): Int =
    Ordering[Double].compare(x._2, y._2)
}

private class MicroClusterInfo(
                                var centroid: breeze.linalg.Vector[Double],
                                var rmsd: Double,
                                var n: Long) extends Serializable {

  def setCentroid(centroid: Vector[Double]): Unit = {
    this.centroid = centroid
  }

  def setRmsd(rmsd: Double): Unit = {
    this.rmsd = rmsd
  }

  def setN(n: Long): Unit = {
    this.n = n
  }
}

private object CluStreamOnline {
  private val RANDOM = "random"
  private val KMEANS = "kmeans"
}