package com.backhoff.clustream

/**
 * Created by omar on 9/14/15.
 */
/* SimpleApp.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

object SimpleApp {
  def timer[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // call-by-name
    val t1 = System.nanoTime()
    println("Elapsed time: " + (t1 - t0) / 1000000 + "ms")
    result
  }
  def main(args: Array[String]) {
//    val logFile = "/home/omar/Libs/spark-1.5.0/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
//    val logData = sc.textFile(logFile, 2).cache()
//    val numAs = logData.filter(line => line.contains("a")).count()
//    val numBs = logData.filter(line => line.contains("b")).count()
//    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))

    val clustream = new CluStream(null)
    val snap1 = timer{clustream.getMCsFromSnapshots("snaps",7,1)}
    val snap2 = timer{clustream.getMCsFromSnapshots("snaps",23,1)}
    val snap3 = timer{clustream.getMCsFromSnapshots("snaps",85,1)}
    val snap4 = timer{clustream.getMCsFromSnapshots("snaps",168,1)}

    val clus = timer{clustream.fakeKMeans(sc,5,5000,snap1)}
    if(clus != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",7,1))
      clus.clusterCenters.foreach(println)
    }
    val clusters2 = timer{clustream.fakeKMeans(sc,5,5000,snap2)}
    if(clusters2 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",23,1))
      clusters2.clusterCenters.foreach(println)
    }
    val clusters3 = timer{clustream.fakeKMeans(sc,5,5000,snap3)}
    if(clusters3 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",85,1))
      clusters3.clusterCenters.foreach(println)
    }
    val clusters4 = timer{clustream.fakeKMeans(sc,5,5000,snap4)}
    if(clusters4 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",168,1))
      clusters4.clusterCenters.foreach(println)
    }
   // val clu = new CluStream().setK(23).setH(100)

   // println("CluStream with " + clu.k + " clusters and a horizon of " + clu.h)

  }
}
