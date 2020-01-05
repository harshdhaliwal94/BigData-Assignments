package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

import collection.mutable.HashMap

class StripesConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, reducers)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}


object ComputeBigramRelativeFrequencyStripes extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    def main(argv: Array[String]){
        val args = new StripesConf(argv)
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of reducers: " + args.reducers())

        val conf = new SparkConf().setAppName("Bigram Relative Frequency Stripes")
        val sc = new SparkContext(conf)

        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        val textFile = sc.textFile(args.input())
        textFile
            .flatMap(line => {
                val tokens = tokenize(line)
                if(tokens.length>1) {
                    tokens.sliding(2).map( t => (t.head, Map(t.last -> 1.0) ) )
                }
                else List()
            })
            .reduceByKey((stripeMapA, stripeMapB)=>{
                stripeMapA ++ stripeMapB.map{ case (k,v) => (k, v + stripeMapA.getOrElse(k,0.0) )}
            })
            .map(s =>{
                val sum = s._2.foldLeft(0.0)(_+_._2)
                (s._1, s._2.map{ case (k,v) => k + "=" + v/sum}) 
            })
            .map( t => "("+t._1 + ", {" + t._2.mkString(", ") + "} )" )
            .saveAsTextFile(args.output())
    }
}
