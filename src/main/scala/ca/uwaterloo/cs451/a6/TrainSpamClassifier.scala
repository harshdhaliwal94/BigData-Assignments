package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

import scala.collection.mutable
import scala.math.exp

class TrainConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, model, shuffle)
    val input = opt[String](descr = "input path", required = true)
    val model = opt[String](descr = "model path", required = true)
    val shuffle = opt[Boolean](descr = "data shuffling", required = false)
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object TrainSpamClassifier {
    val log = Logger.getLogger(getClass().getName())
    
    //w is the weight vector
    val w = mutable.Map[Int, Double]()

    val delta = 0.002

    //scores a document based on its list of features
    def spamminess(features: Array[Int]) : Double = {
        var score =0d
        features.foreach( f => if(w.contains(f)) score += w(f) )
        score
    }

    def main(argv: Array[String]){
        val args = new TrainConf(argv)
        log.info("Input: "+args.input())
        log.info("Model: "+args.model())
        //get input from text file

        val conf = new SparkConf().setAppName("TrainSpamClassifier")
        val sc = new SparkContext(conf)

        var textFile = sc.textFile(args.input())

        //shuffle data if flag present
        if(args.shuffle()){
            val r = scala.util.Random
            textFile = textFile
                        .map(line => {
                            val rand = r.nextInt
                            (rand,line)
                        })
                        .sortByKey()
                        .map( x => x._2)
        }

        //Train Classifier
        val trained = textFile.map( line => {
            //Parse input
            val tokens = line.split(" ")
            val docId = tokens(0)
            val isSpam : Int = if(tokens(1).equals("spam")) 1 else 0
            val feats = tokens.slice(2,tokens.size).map( x => x.toInt )
            (0, (docId, isSpam, feats))
        })
        .groupByKey(1)
        .flatMap( instances => {
            instances._2.foreach( instance => {
                val isSpam = instance._2
                val features = instance._3
                val score = spamminess(features)
                val prob = 1.0/( 1 + exp(-score) )
                features.foreach( f => {
                    if(w.contains(f)){
                        w(f) += (isSpam - prob) * delta
                    } else {
                        w(f) = (isSpam - prob) * delta
                    }
                })
            })
            w
        })
        
        //save model as textfile
        val fs= FileSystem.get(sc.hadoopConfiguration)
        val outPutPath = new Path(args.model())
        if (fs.exists(outPutPath))
            fs.delete(outPutPath, true)
        trained.saveAsTextFile(args.model())
    }
}
