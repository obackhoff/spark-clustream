package com.backhoff.clustream

/**
 * Created by omar on 9/18/15.
 */

import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.clustering.StreamingKMeans

object StreamingKMeans {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming K-means test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Milliseconds(1000))
//    val trainingData = ssc.textFileStream("file:///home/omar/stream/train").map(_.split(" ")).map(arr => arr.dropRight(1)).map(_.mkString("[", ",", "]")).map(Vectors.parse)
//    val trainingData = ssc.socketTextStream("localhost",9999).map(_.split(" ")).map(arr => arr.dropRight(1)).map(_.mkString("[",",","]")).map(Vectors.parse)
    val trainingData = ssc.socketTextStream("localhost",9999).map(_.split(" ")).map(_.mkString("[",",","]")).map(Vectors.parse)
    //val testData = ssc.textFileStream("/home/omar/stream/testing").map(LabeledPoint.parse)
   // val testData = ssc.socketTextStream("localhost", 9998).map(LabeledPoint.parse)
    val numDimensions = 34
    val numClusters = 5
    val model = new StreamingKMeans()
      .setK(numClusters)
      .setHalfLife(1000, "points")
      //.setDecayFactor(0.0)
      .setRandomCenters(numDimensions, 0.0)

    val N = new StaticVar[Long](0L)
    val listener = new MyListener(model, N)
    ssc.addStreamingListener(listener)

    model.trainOn(trainingData)
    //model.predictOnValues(testData.map(lp => (lp.label, lp.features))).print()

    ssc.start()
    ssc.awaitTermination()
  }
}

private[clustream] class MyListener(model: StreamingKMeans, n: StaticVar[Long]) extends StreamingListener {
  override def onBatchCompleted(batchCompleted:StreamingListenerBatchCompleted) {
    if ( batchCompleted.batchInfo.numRecords > 0) {
      n.value = n.value + batchCompleted.batchInfo.numRecords
      println("================= CENTERS ================= N = " + n.value)
      model.latestModel().clusterCenters.foreach(println)
    }
  }
}
class StaticVar[T]( var value: T )
