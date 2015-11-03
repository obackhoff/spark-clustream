package com.backhoff.clustream

/**
 * Created by omar on 9/25/15.
 */

import breeze.linalg._
import breeze.stats.distributions.Gaussian
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.{DeveloperApi, Experimental, Since}


@Experimental
class CluStreamModel(
                      val q: Int,
                      val alpha: Int,
                      val alphaModifier: Int,
                      val numDimensions: Int,
                      val minInitPoints: Int)
  extends Logging with Serializable {

  private var time: Long = 0L
  private var N: Long = 0L

  private var microClusters: Array[MicroCluster] = null
  private val centroids: Array[(breeze.linalg.Vector[Double], Int)]  = Array.fill(q)(Vector.fill[Double](numDimensions)(0)) zip (0 until q)
  private val numPoints: Array[(Long, Int)] = Array.fill(q)(0L) zip (0 until q)

  private var broadcastCentroids: Broadcast[Array[(breeze.linalg.Vector[Double], Int)]] = null
  private var broadcastNumPoints: Broadcast[Array[(Long, Int)]] = null
  private var broadcastQ: Broadcast[Int] = null
  private var broadcastRMSD: Broadcast[Double] = null

  private var initialized = false
  private var initArr: Array[breeze.linalg.Vector[Double]] = Array()

  private def initRand(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {
    broadcastCentroids = rdd.context.broadcast(Array.fill(q)(Vector.fill[Double](numDimensions)(rand())) zip (0 until q))
    broadcastQ = rdd.context.broadcast(q)
    broadcastNumPoints = rdd.context.broadcast(numPoints)
    microClusters = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0), Vector.fill[Double](numDimensions)(0), 0L, 0L, 0L))
    initialized = true
  }

  private def initKmeans(rdd: RDD[breeze.linalg.Vector[Double]]): Unit = {
    initArr = initArr ++ rdd.collect
    if (initArr.length >= minInitPoints) {

      import org.apache.spark.mllib.clustering.KMeans
      val clusters = KMeans.train(rdd.context.parallelize(initArr.map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))), q, 20)

      for (i <- clusters.clusterCenters.indices) centroids(i) = (DenseVector(clusters.clusterCenters(i).toArray), centroids(i)._2 )

      microClusters = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0), Vector.fill[Double](numDimensions)(0), 0L, 0L, 0L))
      val assignations = assignToMicroCluster(rdd.context.parallelize(initArr), q, centroids)
      updateMicroClusters(assignations)

      var i = 0
      for (mc <- this.microClusters) {
        if (mc.getN > 0) centroids(i) = (mc.getCf1x :/ mc.getN.toDouble, centroids(i)._2 )
        numPoints(i) = (mc.getN.toLong, numPoints(i)._2 )
        i += 1
      }
      broadcastCentroids = rdd.context.broadcast(centroids)
      broadcastNumPoints = rdd.context.broadcast(numPoints)
      broadcastQ = rdd.context.broadcast(q)

      initialized = true
    }
  }

  def run(data: DStream[breeze.linalg.Vector[Double]]): Unit = {
    data.foreachRDD { (rdd, time) =>
      this.time += 1
      this.N += rdd.count()
      if (!rdd.isEmpty()) {

        if (initialized) {

          val assignations = assignToMicroCluster(rdd, broadcastQ.value, broadcastCentroids.value)
          updateMicroClusters(assignations)
          var i = 0
          for (mc <- this.microClusters) {
            if (mc.getN > 0) centroids(i) = (mc.getCf1x :/ mc.getN.toDouble, centroids(i)._2 )
            numPoints(i) = (mc.getN.toLong, numPoints(i)._2 )
            i += 1
          }
          broadcastCentroids = rdd.context.broadcast(centroids)
          broadcastNumPoints = rdd.context.broadcast(numPoints)

          //PRINT STUFF FOR DEBUGING
          microClusters.foreach {mc =>
            println("IDs " + mc.getIds.mkString(" "))
            println("CF1X: " + mc.getCf1x.toString)
            println("CF2X: " + mc.getCf2x.toString)
            println("CF1T: " + mc.getCf1t.toString)
            println("CF2T: " + mc.getCf2t.toString)
            println("N: " + mc.getN.toString)
            println()
          }
          println("Centers: ")
          broadcastCentroids.value.foreach(println)
          println("Total time units elapsed: " + this.time)
          println("Total number of points: " + N)
          println("N alternativo: ")
          broadcastNumPoints.value.foreach(println)


        } else { minInitPoints match {
          case 0 => initRand(rdd)
          case _ => initKmeans(rdd) }
        }

      }
    }
  }

  private def saveSnapshot(): Unit = {}

  private def mergeMicroClusters(): Unit = {}

  private def assignToMicroCluster(rdd: RDD[Vector[Double]], q: Int, centroids: Array[(Vector[Double], Int)]): RDD[(Int, Vector[Double])] = {
    rdd.map { a =>
      val arr = Array.fill[(Int,Double)](q)(0,0)
      var i = 0
      for (c <- centroids) {
        arr(i) = (c._2, squaredDistance(a, c._1))
        i += 1
      }
      (arr.min(new OrderingDoubleTuple)._1, a)
    }
  }

  private def updateMicroClusters(assignations: RDD[(Int, Vector[Double])]): Unit = {

//    if(initialized) {
//      val rmsd = assignations.map { a =>
//        val nearMC = microClusters.find(mc => mc.getIds(0) == a._1).get
//        if (nearMC.getN > 1) {
//          val mcCenter = nearMC.getCf1x :/ nearMC.getN.toDouble
//          (scala.math.sqrt((1.0 / nearMC.getN.toDouble) * ((mcCenter - a._2) dot (mcCenter - a._2))), a._2)
//        } else
//          (scala.math.sqrt(squaredDistance(a._2, broadcastCentroids.value.find(c => c._2 == a._1).get._1)), a._2)
//      }
//      print("RMSD!!!!!!")
//      rmsd.foreach(println)
//      println("END RMSD!!!!")
//    }
    if(initialized) {
      val rmsd = assignations.map { a =>
        val numP = broadcastNumPoints.value.find(id => id._2 == a._1).get._1
        val mcCenter = broadcastCentroids.value.find(id => id._2 == a._1).get._1
        if (numP > 1) {
          (scala.math.sqrt((1.0 / numP) * ((mcCenter - a._2) dot (mcCenter - a._2))), a._2)
        } else
          (scala.math.sqrt(squaredDistance(a._2, mcCenter)), a._2)
      }
      print("RMSD!!!!!!")
      rmsd.foreach(println)
      println("END RMSD!!!!")
    }



    val pointCount = assignations.groupByKey().mapValues(a => a.size).collect()
    val sums = assignations.reduceByKey(_ :+ _).collect()
    val sumsSquares = assignations.mapValues(a => a :* a).reduceByKey(_ :+ _).collect()

    for (mc <- this.microClusters) {
      for (s <- sums) if (mc.getIds(0) == s._1) mc.setCf1x(mc.cf1x :+ s._2)
      for (ss <- sumsSquares) if (mc.getIds(0) == ss._1) mc.setCf2x(mc.cf2x :+ ss._2)
      for (pc <- pointCount) if (mc.getIds(0) == pc._1) {
        mc.setN(mc.n + pc._2)
        mc.setCf1t(mc.cf1t + pc._2 * this.time )
        mc.setCf2t(mc.cf2t + pc._2 * (this.time * this.time) )
      }
    }

  }

}

private object MicroCluster extends Serializable {
  private var current = -1

  private def inc = {
    current += 1
    current
  }
}

private class MicroCluster(
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

private object CluStreamModel {
  private val RANDOM = "random"
  private val KMEANS = "kmeans"
}