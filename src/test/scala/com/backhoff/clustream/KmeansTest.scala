package com.backhoff.clustream

/**
 * Created by omar on 10/7/15.
 */

import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KmeansTest {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("K-means test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")

    val data = sc.textFile("/home/omar/stream/streamMod")
    val parsedData = data.map(s => Vectors.dense(s.split(' ').dropRight(1).map(_.toDouble))).cache()

    // Cluster the data into two classes using KMeans
    val numClusters = 2
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    // Evaluate clustering by computing Within Set Sum of Squared Errors
    val WSSSE = clusters.computeCost(parsedData)
    println("Within Set Sum of Squared Errors = " + WSSSE)

    // Save and load model
//    clusters.save(sc, "/home/omar/Desktop/model")
//    val sameModel = KMeansModel.load(sc, "/home/omar/Desktop/model")
    clusters.clusterCenters.foreach(println)
  }
}
