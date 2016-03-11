package org.apache.spark.streamdm

/**
 * Created by omar on 9/18/15.
 */

import com.github.javacliparser.ClassOption
import org.apache.spark.streamdm.clusterers.{Clusterer, Clustream}
import org.apache.spark.streamdm.evaluation.Evaluator
import org.apache.spark.streamdm.streams.{StreamWriter, StreamReader}
import org.apache.spark.streamdm.tasks.Task
import org.apache.spark.streaming.scheduler.{StreamingListenerBatchCompleted, StreamingListener}
import org.apache.spark.streaming.{Milliseconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}

class Clus extends Task {
  //Task options
  val evaluatorOption:ClassOption = new ClassOption("evaluator", 'e',
    "Evaluator to use", classOf[Evaluator], "ClusteringCohesionEvaluator")
  val clustererOption:ClassOption = new ClassOption("learner", 'l',
    "Learner to use", classOf[Clustream], "Clustream")
  val streamReaderOption:ClassOption = new ClassOption("streamReader", 's',
    "Stream reader to use", classOf[StreamReader], "SocketTextStreamReader")
  val resultsWriterOption:ClassOption = new ClassOption("resultsWriter", 'w',
    "Stream writer to use", classOf[StreamWriter], "PrintStreamWriter")

  //Run the task
  def run(ssc:StreamingContext): Unit = {
    //Parse options and init
    val reader:StreamReader = this.streamReaderOption.getValue()
    val clusterer:Clustream = this.clustererOption.getValue()
    clusterer.init(reader.getExampleSpecification())
    val writer:StreamWriter = this.resultsWriterOption.getValue()
    val evaluator:Evaluator = this.evaluatorOption.getValue()

    //clusterer.microclusters.horizonOption.setValue(1)
    clusterer.initOption.setValue(2000)
    clusterer.kOption.setValue(5)
    clusterer.mcOption.setValue(50)
    clusterer.repOption.setValue(10)

    //Parse stream and get Examples
    val N = new StaticVar[Long](0L)
    val listener = new MyListener(clusterer, N)
    ssc.addStreamingListener(listener)
    val instances = reader.getExamples(ssc)

    //Predict
   // val predPairs = learner.predict(instances)
    //Train
    clusterer.train(instances)
    //Assign
    val clpairs = clusterer.assign(instances)

    //Print statistics
    writer.output(evaluator.addResult(clpairs))
  }
}

class MyListener(model: Clustream, n: StaticVar[Long]) extends StreamingListener {
  override def onBatchCompleted(batchCompleted:StreamingListenerBatchCompleted) {
    if ( batchCompleted.batchInfo.numRecords > 0) {
      n.value = n.value + batchCompleted.batchInfo.numRecords
      println("================= CENTERS ================= N = " + n.value)
      model.clusters.foreach(c => println(c.toString()))
      println(model.microclusters.horizonOption.getValue)
    }
  }
}
class StaticVar[T]( var value: T )

object StreamDM {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Streaming K-means test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("ERROR")
    val ssc = new StreamingContext(sc, Milliseconds(1000))


    val numDimensions = 34
    val numClusters = 5
    val task = new Clus()
    task.run(ssc)
    ssc.start()
    ssc.awaitTermination()
  }
}
