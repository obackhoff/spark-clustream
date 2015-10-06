package com.backhoff.clustream

/**
 * Created by omar on 9/25/15.
 */

import breeze.linalg._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.util.Utils
import org.apache.spark.util.random.XORShiftRandom

@Experimental
class CluStreamModel (
                       val q: Int,
                       val alpha: Int,
                       val alphaModifier: Int,
                       val numDimensions: Int,
                       val sc: SparkContext)
  extends Logging with Serializable{

  private var time: Long = 0L
  private var cf2x: RDD[breeze.linalg.Vector[Double]] = null
  private var cf1x: RDD[breeze.linalg.Vector[Double]] = null
  private var microClusters: Array[MicroCluster] = null
  private val centroids: Array[breeze.linalg.Vector[Double]] = Array.fill(q)(Vector.fill[Double](numDimensions)(rand()))
  private var N: Long = 0L
  private var broadcastCentroids: Broadcast[Array[breeze.linalg.Vector[Double]]] = null
  private var broadcastQ: Broadcast[Int] = null
  private var testMC: RDD[MicroCluster] = null

  initialize()

  def update(rdd: RDD[breeze.linalg.Vector[Double]]): Unit ={
//    val cf2xPairs = cf2x.zipWithIndex().map(a => (a._2,a._1))
//    val cf1xPairs = cf1x.zipWithIndex().map(a => (a._2,a._1))
//    val squares = rdd.map(a => a :* a)
//
//    cf1x.unpersist()
//    cf2x.unpersist()
//
//    cf1x = cf1xPairs.union(rdd.zipWithIndex().map(a => (a._2,a._1))).reduceByKey(_ :+ _).map(a => a._2)
//    cf2x = cf2xPairs.union(squares.zipWithIndex().map(a => (a._2,a._1))).reduceByKey(_ :+ _).map(a => a._2)


    val assignations = assignToMicroCluster(rdd,broadcastQ.value, broadcastCentroids.value)
    //updateMicroClusters(assignations)


    val i = 0
    for(mc <- microClusters){
      if(mc.getN > 0) centroids(i) = mc.getCf1x :/ mc.getN.toDouble
    }
    broadcastCentroids = rdd.context.broadcast(centroids)

  }
  def initialize(): Unit ={
    cf2x = sc.parallelize(Array.fill(q)(Vector.zeros[Double](numDimensions)))
    cf1x = sc.parallelize(Array.fill(q)(Vector.zeros[Double](numDimensions)))
    microClusters = Array.fill(q)(new MicroCluster(Vector.fill[Double](numDimensions)(0),Vector.fill[Double](numDimensions)(0),0L,0L,0L))
    broadcastCentroids = sc.broadcast(centroids)
    broadcastQ = sc.broadcast(q)
    testMC = sc.parallelize(microClusters)

//    cf2t = sc.parallelize(Array.fill(q)(Vector.zeros[Double](numDimensions)))
//    cf1t = sc.parallelize(Array.fill(q)(Vector.zeros[Double](numDimensions)))
//    n = sc.parallelize(Array.fill[Long](q)(0))
  }
  def run(data: DStream[breeze.linalg.Vector[Double]]): Unit ={
    data.foreachRDD { (rdd, time) =>
      this.time += 1
      if(!rdd.isEmpty()) {
        //update(rdd: RDD[breeze.linalg.Vector[Double]])
        val assignations = assignToMicroCluster(rdd,broadcastQ.value, broadcastCentroids.value)
        updateMicroClusters(assignations)
        var i = 0
        for(mc <- this.microClusters){
          if(mc.getN > 0) centroids(i) = mc.getCf1x :/ mc.getN.toDouble
          i += 1
        }
        broadcastCentroids = rdd.context.broadcast(centroids)

        this.N += rdd.count()

//        println()
//        cf1x = sc.parallelize(Array(cf1x.reduce(_ :+ _)))
//        print("CF1X: ")
//        cf1x.foreach(println)
//        cf2x = sc.parallelize(Array(cf2x.reduce(_ :+ _)))
//        print("CF2X: ")
//        cf2x.foreach(println)
        println("CF1X: ")
        microClusters.foreach(a => println(a.getCf1x))
        println("CF2X: ")
        microClusters.foreach(a => println(a.getCf2x))
        println("number of points per MC: ")
        microClusters.foreach(a => println(a.getN))
        println("Centers: ")
        broadcastCentroids.value.foreach(println)
        println("Total time units elapsed: " + this.time)
        println("Total number of points: " + N)
        println()

      }
    }
  }

  def saveSnapshot(): Unit ={}
  def mergeMicroClusters(): Unit ={}
  def assignToMicroCluster(rdd: RDD[Vector[Double]],q: Int, centroids: Array[Vector[Double]]): RDD[(Int,Vector[Double])] ={
    rdd.map{ a =>
      val arr = Array.fill[(Int,Double)](q)(0,0)
      var i = 0
      for(c <- centroids){
        arr(i) = (i, squaredDistance(a, c))
        i += 1
      }
      (arr.min(new OrderingDoubleTuple)._1, a)
    }
  }

  def updateMicroClusters(assignations: RDD[(Int,Vector[Double])]): Unit ={
    val pointCount = assignations.groupByKey.mapValues(a => a.size).collect
    val sums = assignations.reduceByKey(_ :+ _).collect
    val sumsSquares = assignations.mapValues(a => a :* a).reduceByKey(_ :+ _).collect

//    pointCount.foreach(println)
//    sums.foreach(println)
//    sumsSquares.foreach(println)
    for(mc <- this.microClusters){
      for(s <- sums) if(mc.getIds(0) == s._1) mc.setCf1x(mc.cf1x :+ s._2)
      for(ss <- sumsSquares) if(mc.getIds(0) == ss._1) mc.setCf2x(mc.cf2x :+ ss._2)
      for(pc <- pointCount) if(mc.getIds(0) == pc._1) mc.setN(mc.n + pc._2)
    }
  }

}


private object MicroCluster extends Serializable{
  private var current = -1
  private def inc = {current += 1; current}
}

private class MicroCluster(
                            var cf2x: breeze.linalg.Vector[Double],
                            var cf1x: breeze.linalg.Vector[Double],
                            var cf2t: Long,
                            var cf1t: Long,
                            var n: Long,
                            var ids: Array[Int]) extends Serializable{

  def this(cf2x: breeze.linalg.Vector[Double],cf1x: breeze.linalg.Vector[Double],cf2t: Long,cf1t: Long, n: Long) = this(cf2x,cf1x,cf2t,cf2t,n, Array(MicroCluster.inc))

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

private class OrderingMicroCluster extends Ordering[Tuple2[Array[Int], Double]] {
  override def compare(x: (Array[Int], Double), y: (Array[Int], Double)): Int =
    Ordering[Double].compare(x._2, y._2)
}

private class OrderingDoubleTuple extends Ordering[Tuple2[Int, Double]] with Serializable{
  override def compare(x: (Int, Double), y: (Int, Double)): Int =
    Ordering[Double].compare(x._2, y._2)
}
