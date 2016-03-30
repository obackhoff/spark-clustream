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
    val t1 = 6
    val t2 = 21
    val t3 = 81
    val t4 = 161

//    Tools.convertMCsBinariesToText("snaps", "snaps/text", 100)
    val clustream = new CluStream(null)
    val snap1 = timer{clustream.getMCsFromSnapshots("snaps",t1,h)}
    val snap2 = timer{clustream.getMCsFromSnapshots("snaps",t2,h)}
    val snap3 = timer{clustream.getMCsFromSnapshots("snaps",t3,h)}
    val snap4 = timer{clustream.getMCsFromSnapshots("snaps",t4,h)}

    println(snap1.map(a => a.getN).mkString("[",",","]"))
    println("mics points = " + snap1.map(_.getN).sum)
    println(snap2.map(a => a.getN).mkString("[",",","]"))
    println("mics points = " + snap2.map(_.getN).sum)
    println(snap3.map(a => a.getN).mkString("[",",","]"))
    println("mics points = " + snap3.map(_.getN).sum)
    println(snap4.map(a => a.getN).mkString("[",",","]"))
    println("mics points = " + snap4.map(_.getN).sum)

    val clusters1 = timer{clustream.fakeKMeans(sc,5,5000,snap1)}
    if(clusters1 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",t1,h))
      clusters1.clusterCenters.foreach(println)
      clusters1.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/1case/results/clustream2000/centers1").createFile().appendAll(c.toArray.mkString("",",","") +"\n" ))
    }
    val clusters2 = timer{clustream.fakeKMeans(sc,5,5000,snap2)}
    if(clusters2 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",t2,h))
      clusters2.clusterCenters.foreach(println)
      clusters2.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/1case/results/clustream2000/centers2").createFile().appendAll(c.toArray.mkString("",",","") +"\n" ))
    }
    val clusters3 = timer{clustream.fakeKMeans(sc,5,5000,snap3)}
    if(clusters3 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",t3,h))
      clusters3.clusterCenters.foreach(println)
      clusters3.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/1case/results/clustream2000/centers3").createFile().appendAll(c.toArray.mkString("",",","") +"\n" ))
    }
    val clusters4 = timer{clustream.fakeKMeans(sc,5,5000,snap4)}
    if(clusters4 != null) {
      println("MacroClusters Ceneters")
      println("snapshots " + clustream.getSnapShots("snaps",t4,h))
      clusters4.clusterCenters.foreach(println)
      clusters4.clusterCenters.foreach(c=>scala.tools.nsc.io.Path("/home/omar/datasets/tests/1case/results/clustream2000/centers4").createFile().appendAll(c.toArray.mkString("",",","") +"\n" ))
    }


  }
}
