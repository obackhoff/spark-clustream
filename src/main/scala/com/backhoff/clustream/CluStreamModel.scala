package com.backhoff.clustream

/**
 * Created by omar on 9/25/15.
 */

import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
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

  private val time: Long = 0L
  private val cf2x: Array[Vector] = Array.fill(q)(Vectors.zeros(numDimensions))
  private val cf1x: Array[Vector] = Array.fill(q)(Vectors.zeros(numDimensions))
  private val cf2t: Array[Vector] = Array.fill(q)(Vectors.zeros(numDimensions))
  private val cf1t: Array[Vector] = Array.fill(q)(Vectors.zeros(numDimensions))
  private val n: Array[Long] = Array.fill(q)(0L)

  def initialize(): Unit ={}
  def run(): Unit ={}
  def saveSnapshot(): Unit ={}
  def mergeMicroClusters(): Unit ={}
  def assignToMicroCluster(): Unit ={}
}
