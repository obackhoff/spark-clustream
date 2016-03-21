package com.backhoff.clustream

/**
  * Created by omar on 9/20/15.
  */

import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

import breeze.linalg._

object StreamingTests {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Stream Word Count").setMaster("local[*]")
    //    val conf = new SparkConf().setAppName("Stream Word Count").setMaster("spark://192.168.0.119:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Milliseconds(1000))
    // ssc.checkpoint("/home/omar/stream/checkpoint")
    val lines = ssc.socketTextStream("localhost", 9999)
    //    val lines = ssc.textFileStream("file:///home/omar/stream/train")

    //    val words = lines.flatMap(_.split(" ").map(_.toInt))
    //    val pairs = words.map(word => (word, 1))
    //    val wordCounts = pairs.reduceByKey(_ + _)
    //
    //
    //    wordCounts.print()

    //    val words = lines.map(_.split(" ").map(_.toInt).zipWithIndex)
    //    val pairs = words.flatMap(a => a).transform(_.map(a => (a._2,a._1)))
    //    val wordCounts = pairs.reduceByKey(_ + _)


    val model = new CluStreamOnline(50, 34, 2000).setDelta(1).setM(1000).setRecursiveOutliersRMSDCheck(true)
    val clustream = new CluStream(model)
    ssc.addStreamingListener(new PrintClustersListener(clustream, sc))
    //    model.run(lines.map(_.split(" ").map(_.toDouble)).map(DenseVector(_)))
    //    clustream.startOnline(lines.map(_.split(" ").map(_.toDouble)).map(arr => arr.dropRight(1)).map(DenseVector(_)))
    clustream.startOnline(lines.map(_.split(" ").map(_.toDouble)).map(DenseVector(_)))

    // wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }

}

private[clustream] class PrintClustersListener(clustream: CluStream, sc: SparkContext) extends StreamingListener {
  def timer[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
    val tc = clustream.model.getCurrentTime
    val n = clustream.model.getTotalPoints

//    clustream.saveSnapShotsToDisk("snaps",tc, 2, 10)
          println("tc = " + tc + ", n = " + n)

    if (batchCompleted.batchInfo.numRecords > 0 ) {

//      if (Array(750,1250,1750,2250).contains(tc)) {
//        val snaps = clustream.getSnapShots("snaps",tc,1)
        val clusters = timer {
//          clustream.fakeKMeans(sc, 5, 2000, clustream.getMCsFromSnapshots("snaps", tc, 1))
          clustream.fakeKMeans(sc, 5, 2000, clustream.model.getMicroClusters)
        }
//        if (clusters != null) {
//          println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
          clusters.clusterCenters.foreach(println)
//        }
//      }

    }
  }
}
