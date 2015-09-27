package com.backhoff.clustream

/**
 * Created by omar on 9/25/15.
 */

import breeze.linalg._
import org.apache.spark.Logging
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.rdd.RDD
import org.apache.spark.annotation.{Experimental, Since}
import org.apache.spark.util.Utils

@Experimental
class CluStreamModel (
                       val q: Int,
                       val alpha: Int,
                       val alphaModifier: Int,
                       val numDimensions: Int)
  extends Logging{

//  private var time: Long = 0L
//  private var cf2x: Array[Vector[Double]] = Array.fill(q)(Vector.zeros[Double](numDimensions))
//  private var cf1x: Array[Vector[Double]] = Array.fill(q)(Vector.zeros[Double](numDimensions))
//  private var cf2t: Array[Vector[Double]] = Array.fill(q)(Vector.zeros[Double](numDimensions))
//  private var cf1t: Array[Vector[Double]] = Array.fill(q)(Vector.zeros[Double](numDimensions))
//  private var n: Vector[Long] = Vector.zeros[Long](q)

  def update(): Unit ={

  }
  def initialize(): Unit ={}
  def run(): Unit ={}
  def saveSnapshot(): Unit ={}
  def mergeMicroClusters(): Unit ={}
  def assignToMicroCluster(): Unit ={}
//  def joinRDDs(data: DStream): RDD[Vector] ={
//    var temp: RDD[] = null
//    for(i <- data.count()){
//      temp = data.foreachRDD()
//    }
//  }
}
