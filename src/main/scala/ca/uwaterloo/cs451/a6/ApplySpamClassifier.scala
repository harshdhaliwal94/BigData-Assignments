package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

import scala.collection.mutable
import scala.math.exp

class ApplyConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, model)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val model = opt[String](descr = "model path", required = true)
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object ApplySpamClassifier{
    val log = Logger.getLogger(getClass().getName())
    
    val pattern = """\((\d+),(-?\d+\.\d+(E-?\d+)?)\)""".r
    
    def main(argv : Array[String]){
        val args = new ApplyConf(argv)
        log.info("Input: "+args.input())
        log.info("Output: "+args.output())
        log.info("Model: "+args.model())

        val conf = new SparkConf().setAppName("ApplySpamClassifier")
        val sc = new SparkContext(conf)

        //load model
        val model = sc.textFile(args.model())
        val weights = model
            .map(line =>{
                val p = try{line match { case pattern(p1,p2,p3) => (p1,p2,p3)}} catch{case e : Throwable => null}
                if(p==null) System.exit(1)
                if(p._1==null || p._2==null) System.exit(1)
                (p._1.toInt,p._2.toDouble)
            })
            .collectAsMap()
        val w = sc.broadcast(weights)

        //scores a document based on its list of features
        def spamminess(features: Array[Int]) : Double = {
            var score =0d
            features.foreach( f => if(w.value.contains(f)) score += w.value(f) )
            score
        }

        //get input from text file
        val textFile = sc.textFile(args.input())
        val test = textFile
            .map( line => {
                //Parse input
                val tokens = line.split(" ")
                val docId = tokens(0)
                val label = tokens(1)
                val features = tokens.slice(2,tokens.size).map( x => x.toInt )
                val score = spamminess(features)
                val predict : String = if(score>0) "spam" else "ham"
                (docId,label,score,predict)
            })

            //save classification result
            val fs = FileSystem.get(sc.hadoopConfiguration)
            val outputPath = new Path(args.output())
            if (fs.exists(outputPath))
                fs.delete(outputPath, true)
            test.saveAsTextFile(args.output())
    }
}
