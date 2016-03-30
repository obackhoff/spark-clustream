package com.backhoff.clustream

/**
  * Created by omar on 9/20/15.
  */

import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import org.apache.log4j._


import breeze.linalg._

object StreamingTests {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark CluStream").setMaster("local[*]")
    //    val conf = new SparkConf().setAppName("Stream Word Count").setMaster("spark://192.168.0.119:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
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


    val model = new CluStreamOnline(50, 34, 2000).setDelta(512).setM(20).setInitNormalKMeans(false)
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

  override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      if (batchCompleted.batchInfo.numRecords > 0) {

        val tc = clustream.model.getCurrentTime
        val n = clustream.model.getTotalPoints

        clustream.saveSnapShotsToDisk("snaps",tc, 2, 10)
        println("tc = " + tc + ", n = " + n)

//      if (149900 < n && n <= 150100 ) {
//
//        val snaps = clustream.getSnapShots("snaps",tc,256)
//        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.getMCsFromSnapshots("snaps", tc, 256))
//        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
//        clusters.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/2case/results/clustream200/centers1").createFile().appendAll(c.toArray.mkString("",",","") +"\n" ))
//
//
////        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.model.getMicroClusters)
////        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + " ============")
////        clusters.clusterCenters.foreach(println)
//
//      }
//      if( 249900 < n && n <= 250100){
//        val snaps = clustream.getSnapShots("snaps",tc,256)
//        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.getMCsFromSnapshots("snaps", tc, 256))
//        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
//        clusters.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/2case/results/clustream200/centers2").createFile().appendAll(c.toArray.mkString("",",","")+"\n"))
//      }
//      if(349900 < n && n <= 350100 ){
//        val snaps = clustream.getSnapShots("snaps",tc,256)
//        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.getMCsFromSnapshots("snaps", tc, 256))
//        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
//        clusters.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/2case/results/clustream200/centers3").createFile().appendAll(c.toArray.mkString("",",","")+"\n"))
//      }
//      if(449900 < n && n <= 450100){
//        val snaps = clustream.getSnapShots("snaps",tc,256)
//        val clusters = clustream.fakeKMeans(sc, 5, 2000, clustream.getMCsFromSnapshots("snaps", tc, 256))
//        println("=============  MacroClusters Centers for time = " + tc + ", n = " + n + ", snapshots = " + snaps + " ============")
//        clusters.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/2case/results/clustream200/centers4").createFile().appendAll(c.toArray.mkString("",",","")+"\n"))
//      }

    }
  }
}
