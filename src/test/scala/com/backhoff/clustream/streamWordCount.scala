package com.backhoff.clustream

/**
 * Created by omar on 9/20/15.
 */

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming._

object streamWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Stream Word Count").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Seconds(5))
    val lines = ssc.socketTextStream("localhost", 9999)
    val words = lines.flatMap(_.split(" "))
    val pairs = words.map(word => (word, 1))
    val wordCounts = pairs.reduceByKey(_ + _)


    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()
  }
}