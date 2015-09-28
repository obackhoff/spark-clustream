package com.backhoff.clustream

/**
 * Created by omar on 9/25/15.
 */

import breeze.linalg._
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.util.Utils

@Experimental
class CluStreamModel (
                       val q: Int,
                       val alpha: Int,
                       val alphaModifier: Int,
                       val numDimensions: Int,
                       val sc: SparkContext)
  extends Logging {

  private var time: Long = 0L
  private var cf2x: RDD[breeze.linalg.Vector[Double]] = null
  private var cf1x: RDD[breeze.linalg.Vector[Double]] = null
  private var cf2t: RDD[breeze.linalg.Vector[Double]] = null
  private var cf1t: RDD[breeze.linalg.Vector[Double]] = null
  private var n: RDD[Long] = null

  initialize()

  def update(rdd: RDD[breeze.linalg.Vector[Double]]): Unit ={
    val cf2xPairs = cf2x.zipWithIndex().map(a => (a._2,a._1))
    val cf1xPairs = cf1x.zipWithIndex().map(a => (a._2,a._1))
    val squares = rdd.map(a => a :* a)

    cf1x.unpersist()
    cf2x.unpersist()

    cf1x = cf1xPairs.union(rdd.zipWithIndex().map(a => (a._2,a._1))).reduceByKey(_ :+ _).map(a => a._2)
    cf2x = cf2xPairs.union(squares.zipWithIndex().map(a => (a._2,a._1))).reduceByKey(_ :+ _).map(a => a._2)

  }
  def initialize(): Unit ={
    cf2x = sc.parallelize(Array.fill(q)(Vector.zeros[Double](numDimensions)))
    cf1x = sc.parallelize(Array.fill(q)(Vector.zeros[Double](numDimensions)))
//    cf2t = sc.parallelize(Array.fill(q)(Vector.zeros[Double](numDimensions)))
//    cf1t = sc.parallelize(Array.fill(q)(Vector.zeros[Double](numDimensions)))
//    n = sc.parallelize(Array.fill[Long](q)(0))
  }
  def run(data: DStream[breeze.linalg.Vector[Double]]): Unit ={
    data.foreachRDD { (rdd, time) =>
      this.time += 1
      update(rdd: RDD[breeze.linalg.Vector[Double]])

      cf1x = sc.parallelize(Array(cf1x.reduce(_ :+ _)))
      print("CF1X: ")
      cf1x.foreach(println)
      cf2x = sc.parallelize(Array(cf2x.reduce(_ :+ _)))
      print("CF2X: ")
      cf2x.foreach(println)
      println(this.time)
    }
  }
  def saveSnapshot(): Unit ={}
  def mergeMicroClusters(): Unit ={}
  def assignToMicroCluster(point: Vector): Int ={
    0
  }
//  def joinRDDs(data: DStream): RDD[Vector] ={
//    var temp: RDD[] = null
//    for(i <- data.count()){
//      temp = data.foreachRDD()
//    }
//  }
}

private class MicroClusterObject(
                          var cfv: breeze.linalg.Vector[Double],
                          var ids: Array[Int]){

  def this() = this(null,null)

  def setVector(cfv: breeze.linalg.Vector[Double]): this.type = {
    this.cfv = cfv
    this
  }
  def setIds(ids: Array[Int]): this.type = {
    this.ids = ids
    this
  }
  def getVector(): breeze.linalg.Vector[Double] = {
    this.cfv
  }
  def getIds(): Array[Int] = {
    this.ids
  }
}
