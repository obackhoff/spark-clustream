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

    val h = 1
    val t1 = 8
    val t2 = 22
    val t3 = 82
    val t4 = 162

    val clustream = new CluStream(null)
    val snap1 = timer{clustream.getMCsFromSnapshots("snaps",t1,h)}
    val snap2 = timer{clustream.getMCsFromSnapshots("snaps",t2,h)}
    val snap3 = timer{clustream.getMCsFromSnapshots("snaps",t3,h)}
    val snap4 = timer{clustream.getMCsFromSnapshots("snaps",t4,h)}

    val clus = timer{clustream.fakeKMeans(sc,5,5000,snap1)}
    if(clus != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",t1,h))
      clus.clusterCenters.foreach(println)
    }
    val clusters2 = timer{clustream.fakeKMeans(sc,5,5000,snap2)}
    if(clusters2 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",t2,h))
      clusters2.clusterCenters.foreach(println)
    }
    val clusters3 = timer{clustream.fakeKMeans(sc,5,5000,snap3)}
    if(clusters3 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",t3,h))
      clusters3.clusterCenters.foreach(println)
    }
    val clusters4 = timer{clustream.fakeKMeans(sc,5,5000,snap4)}
    if(clusters4 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",t4,h))
      clusters4.clusterCenters.foreach(println)
    }
   // val clu = new CluStream().setK(23).setH(100)

   // println("CluStream with " + clu.k + " clusters and a horizon of " + clu.h)

  }
}
