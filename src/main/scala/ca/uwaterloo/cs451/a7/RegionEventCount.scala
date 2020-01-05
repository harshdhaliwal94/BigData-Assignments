/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

package ca.uwaterloo.cs451.a7

import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{ManualClockWrapper, Minutes, StreamingContext}
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.apache.spark.util.LongAccumulator
import org.rogach.scallop._

import scala.collection.mutable

class RegionEventCountConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object RegionEventCount {
  //create logger
  val log = Logger.getLogger(getClass().getName())
  //Define bounding box  gm=  TL, TR, BR, BL   city= TL, TR, BR, BL
  val goldman : Array[Tuple2[Double,Double]] = Array((-74.0141012, 40.7152191), (-74.013777, 40.7152275), (-74.0141027, 40.7138745), (-74.0144185, 40.7140753))
  val citygroup : Array[Tuple2[Double,Double]] = Array((-74.011869, 40.7217236), (-74.009867, 40.721493), (-74.010140,40.720053), (-74.012083, 40.720267))
  def inBbox(lng_str: String, lat_str: String, loc: String): Boolean = {
      val lng = lng_str.toDouble
      val lat = lat_str.toDouble
      var lat_in: Boolean = false
      var lng_in: Boolean = false
      if(loc.equals("goldman")){
          if((lat<=goldman(0)._2 || lat<=goldman(1)._2) && (lat>=goldman(2)._2 || lat>=goldman(3)._2))
            lat_in = true
          if((lng<=goldman(1)._1 || lat<=goldman(2)._1) && (lng>=goldman(0)._1 || lng>=goldman(3)._1))
            lng_in = true
      }
      else{
          if((lat<=citygroup(0)._2 || lat<=citygroup(1)._2) && (lat>=citygroup(2)._2 || lat>=citygroup(3)._2))
            lat_in = true
          if((lng<=citygroup(1)._1 || lat<=citygroup(2)._1) && (lng>=citygroup(0)._1 || lng>=citygroup(3)._1))
            lng_in = true
      }
      if(lat_in==true && lng_in==true) 
        return true
      else 
        return false
  }
  //Driver function
  def main(argv: Array[String]): Unit = {
    val args = new RegionEventCountConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("RegionEventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val wc = stream.map(_.split(","))
      .filter( tuple => {
          var valid: Boolean = false
          if(tuple(0).equals("green")){
              val at_goldman: Boolean =  inBbox(tuple(8),tuple(9),"goldman")
              val at_city: Boolean =  inBbox(tuple(8),tuple(9),"city")
              if(at_goldman || at_city) valid = true
          }
          else if(tuple(0).equals("yellow")){
              val at_goldman: Boolean =  inBbox(tuple(10),tuple(11),"goldman")
              val at_city: Boolean =  inBbox(tuple(10),tuple(11),"city")
              if(at_goldman || at_city) valid = true
          }
          valid
      })
      .map(tuple => {
          var location: String = "goldman"
          if(tuple(0).equals("green")){
              if(inBbox(tuple(8),tuple(9),"goldman")) location = "goldman"
              else location = "citigroup"
          }
          else{
              if(inBbox(tuple(10),tuple(11),"goldman")) location = "goldman"
              else location = "citigroup"
          }
          (location,1)
      })
      .reduceByKeyAndWindow(
        (x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(60), Minutes(60))
      .persist()

    wc.saveAsTextFiles(args.output())

    wc.foreachRDD(rdd => {
      numCompletedRDDs.add(1L)
    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}
