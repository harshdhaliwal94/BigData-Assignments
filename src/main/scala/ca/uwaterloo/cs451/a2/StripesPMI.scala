package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

import collection.mutable.HashMap

class StripesPMIConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, reducers, threshold)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
    val threshold = opt[Int](descr = "threshold", required = false, default = Some(10))
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object StripesPMI extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    def main(argv: Array[String]){
        val args = new StripesPMIConf(argv)
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of reducers: " + args.reducers())

        val conf = new SparkConf().setAppName("Stripes PMI")
        val sc = new SparkContext(conf)

        val outputDir = new Path(args.output())
        val threshold = args.threshold()
        
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)
        val textFile = sc.textFile(args.input(),args.reducers())
        textFile.cache

        val lineCount = sc.longAccumulator("Line Accumulator")
        val wcMap = textFile
            .flatMap( line => {
                val tokens = tokenize(line)
                if(tokens.length>1){
                    lineCount.add(1)
                    tokens.take(Math.min(40,tokens.length)).toList.distinct
                } else List()
            })
            .map(w => (w,1))
            .reduceByKey(_+_)
            .collectAsMap()

        val broadcastWC = sc.broadcast(wcMap)
        val NUMLINES = lineCount.value.toFloat
        val broadcastLines = sc.broadcast(NUMLINES)

        textFile
            .flatMap( line => {
                val tokens = tokenize(line)
                if(tokens.length>1){
                    val wordList = tokens.take(Math.min(40, tokens.length)).toList.distinct
                    val stripes = for{x<-wordList; y<-wordList; if x!=y} yield (x,Map(y->1.0))
                    stripes.toList
                } else List()
            })
            .reduceByKey((stripeA,stripeB) => {
                stripeA ++ stripeB.map{ case (k,v) => (k, v + stripeA.getOrElse(k,0.0) )}
            })
            .map( stripe => {
                val countX = broadcastWC.value(stripe._1)
                (stripe._1, stripe._2.filter((s) => s._2 >= threshold).map{ 
                    case (k,v) => (k, (Math.log10( (v.toFloat*broadcastLines.value) / (countX*broadcastWC.value(k))),v.toInt) )
                    })
            })
            .filter(stripe => stripe._2.size>0)
            .map(s => {
               "("+ s._1 + "," + " {" + s._2.map{case (k,v) => k+"="+"("+v._1+","+v._2+")"}.mkString(", ") + "}"+")"
            })
            .saveAsTextFile(args.output())
    }
}
