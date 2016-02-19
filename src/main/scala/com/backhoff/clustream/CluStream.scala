package com.backhoff.clustream

/**
 * Created by omar on 9/25/15.
 */

import breeze.linalg._
import org.apache.spark.{SparkContext, Logging}
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.annotation.Experimental
import java.io._
import java.nio.file.{Paths, Files}


@Experimental
class CluStream (
                  var k: Int,
                  var h: Int,
                  val model:CluStreamOnline)
  extends Logging with Serializable{

  def this() = this(2,100,null)

  private def sample[A](dist: Map[A, Double]): A = {
    val p = scala.util.Random.nextDouble
    val it = dist.iterator
    var accum = 0.0
    while (it.hasNext) {
      val (item, itemProb) = it.next
      accum += itemProb
      if (accum >= p)
        return item
    }
    sys.error(f"this should never happen") // needed so it will compile
  }

  def saveSnapShotsToDisk(dir: String = "", alpha: Int = 2, l: Int = 2): Unit ={
    val tc = model.getCurrentTime()
    var write = false
    var delete = false
    var order = 0

    val exp = (scala.math.log(tc)/scala.math.log(alpha)).toInt

    for(i <- 0 to exp){
      if(tc % scala.math.pow(alpha, i + 1) != 0 && tc % scala.math.pow(alpha, i) == 0) {
        order = i
        write = true
      }
    }

    val tcBye = tc - ((scala.math.pow(alpha, l) + 1)*scala.math.pow(alpha, order + 1)).toInt

    if(tcBye > 0 ) delete = true

    if(write) {
      val out = new ObjectOutputStream(new FileOutputStream(dir + "/" + tc))

      try{
        out.writeObject(model.getMicroClusters())
      }
      catch{
        case ex: IOException => println("Exception while writing file " + ex)
      }
      finally{
        out.close()
      }
    }

    if(delete){
      try{
        new File(dir + "/" + tcBye).delete()
      }
      catch{
        case ex: IOException => println("Exception while deleting file " + ex);
      }
    }
  }

  def getSnapShots(dir: String = "", tc: Long, h: Long): (Long,Long) = {

    var tcReal = tc
    while(!Files.exists(Paths.get(dir + "/" + tcReal)) && tcReal >= 0) tcReal = tcReal - 1
    var tcH = tcReal - h
    while(!Files.exists(Paths.get(dir + "/" + tcH)) && tcH >= 0) tcH = tcH - 1

    if(tcReal == -1L) tcH = -1L
    (tcReal, tcH)
  }

  def getCentersFromMC(mcs: Array[MicroCluster]): Array[Vector[Double]] = {
    var arr: Array[Vector[Double]] = Array()
    for(mc <- mcs) {
      val center: Vector[Double] = mc.getCf1x :/ mc.getN.toDouble
      arr = arr :+ center
    }
    arr
  }

  def getWeightsFromMC(mcs: Array[MicroCluster]): Array[Double] = {
    var arr: Array[Double] = Array()
    for(mc <- mcs)
      arr = arr :+ mc.getN.toDouble
    val sum: Double = arr.sum
    arr.map(value => value/sum)
  }

  def fakeKMeans(sc: SparkContext, numPoints: Int): org.apache.spark.mllib.clustering.KMeansModel ={
    if(model.initialized) {
      val kmeans = new org.apache.spark.mllib.clustering.KMeans()
      val mcs = model.getMicroClusters()
      var centers = getCentersFromMC(mcs).map(v => org.apache.spark.mllib.linalg.Vectors.dense(v.toArray))
      val weights = getWeightsFromMC(mcs)
      val map = (centers zip weights).toMap
      val points = Array.fill(numPoints)(sample(map))


      kmeans.setMaxIterations(20)
      kmeans.setK(k)
      kmeans.setInitialModel(new org.apache.spark.mllib.clustering.KMeansModel(Array.fill(k)(sample(map))))
      val trainingSet = sc.parallelize(points)
      val clusters = kmeans.run(trainingSet)
      trainingSet.unpersist(blocking = false)
      clusters
    }else null

  }

  def startOnline(data: DStream[breeze.linalg.Vector[Double]]): Unit ={
    model.run(data)
  }

  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  def setH(h: Int): this.type = {
    this.h = h
    this
  }

}
