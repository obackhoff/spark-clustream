package com.backhoff.clustream

/**
 * Created by omar on 9/20/15.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._
import breeze.linalg._

object StreamingTests {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Stream Word Count").setMaster("local[*]")
//    val conf = new SparkConf().setAppName("Stream Word Count").setMaster("spark://192.168.0.119:7077")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Milliseconds(3000))
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

    val model = new CluStreamModel(2,1,1,2,2000)
    //model.initialize()

//    model.run(lines.map(_.split(" ").map(_.toDouble)).map(DenseVector(_)))
    model.run(lines.map(_.split(" ").map(_.toDouble)).map(arr => arr.dropRight(1)).map(DenseVector(_)))

   // wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}
