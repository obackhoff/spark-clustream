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
class CluStream (
                  var k: Int,
                  var h: Int,
                  val model:CluStreamModel)
  extends Logging with Serializable{

  def this() = this(2,100)

  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  def setH(h: Int): this.type = {
    this.h = h
    this
  }

}
