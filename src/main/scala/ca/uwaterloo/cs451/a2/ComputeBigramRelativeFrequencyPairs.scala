package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

class PairsConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, reducers)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val reducers = opt[Int](descr = "number of reducers", required = false, default = Some(1))
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

class MyPartitionerPairs(parts: Int) extends Partitioner {
    override def numPartitions: Int = parts
    override def getPartition(key: Any): Int = key match {
        case (leftWord, rightWord) => (leftWord.hashCode() & Integer.MAX_VALUE) % parts
        case _ => 0
    }
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    def main(argv: Array[String]){
        val args = new PairsConf(argv)
        log.info("Input: " + args.input())
        log.info("Output: " + args.output())
        log.info("Number of reducers: " + args.reducers())

        val conf = new SparkConf().setAppName("Bigram Relative Frequency Pairs")
        val sc = new SparkContext(conf)

        val outputDir = new Path(args.output())
        FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

        val textFile = sc.textFile(args.input())
        textFile
            .flatMap(line => {
                val tokens = tokenize(line)
                if(tokens.length>1) {
                    val bigram = tokens.sliding(2).map(t => (t.head, t.last)).toList
                    val marginal = bigram.map(t => (t._1,"*"))
                    bigram ++ marginal
                }
                else List()
            })
            .map(bigram => (bigram,1.0))
            .reduceByKey(_ + _)
            .sortByKey()
            .repartitionAndSortWithinPartitions(new MyPartitionerPairs(args.reducers()))
            .mapPartitions(b => {
                var marginalCount = 0.0
                b.map(p=>{
                    if(p._1._2.equals("*")){
                        marginalCount = p._2
                        val bigramFrequency = p
                        bigramFrequency
                    }
                    else{
                        val bigramFrequency = (p._1,p._2/marginalCount)
                        bigramFrequency
                    }
                })                
            })
            .map(b => "((" + b._1._1 + ", " + b._1._2 + "), " + b._2+")")
            .saveAsTextFile(args.output())
    }
}
