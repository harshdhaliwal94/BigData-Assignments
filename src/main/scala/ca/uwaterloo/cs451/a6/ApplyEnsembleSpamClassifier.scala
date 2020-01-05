package ca.uwaterloo.cs451.a6

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._

import scala.collection.mutable
import scala.math.exp

class ApplyEnsembleConf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, output, model, method)
    val input = opt[String](descr = "input path", required = true)
    val output = opt[String](descr = "output path", required = true)
    val model = opt[String](descr = "model path", required = true)
    val method = opt[String](descr = "ensemble mehod", required = true)
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object ApplyEnsembleSpamClassifier{
    val log = Logger.getLogger(getClass().getName())
    
    val pattern = """\((\d+),(-?\d+\.\d+(E-?\d+)?)\)""".r
    
    def main(argv : Array[String]){
        val args = new ApplyEnsembleConf(argv)
        if( !(args.method().equals("average") || args.method().equals("vote")) ){
            println("ERROR: invalid method name")
            System.exit(1)
        }

        log.info("Input: "+args.input())
        log.info("Output: "+args.output())
        log.info("Model: "+args.model())
        log.info("Method: "+args.method())
        
        val conf = new SparkConf().setAppName("ApplyEnsembleSpamClassifier")
        val sc = new SparkContext(conf)

        //load models 
        val model_x = sc.textFile(args.model()+"/part-00000")
        val model_y = sc.textFile(args.model()+"/part-00001")
        val model_britney = sc.textFile(args.model()+"/part-00002")

        val weights_x = model_x
            .map(line =>{
                val p = try{line match { case pattern(p1,p2,p3) => (p1,p2,p3)}} catch{case e : Throwable => null}
                if(p==null) System.exit(1)
                if(p._1==null || p._2==null) System.exit(1)
                (p._1.toInt,p._2.toDouble)
            })
            .collectAsMap()
        val w_x = sc.broadcast(weights_x)
        
        val weights_y = model_y
            .map(line =>{
                val p = try{line match { case pattern(p1,p2,p3) => (p1,p2,p3)}} catch{case e : Throwable => null}
                if(p==null) System.exit(1)
                if(p._1==null || p._2==null) System.exit(1)
                (p._1.toInt,p._2.toDouble)
            })
            .collectAsMap()
        val w_y = sc.broadcast(weights_y)
        
        val weights_britney = model_britney
            .map(line =>{
                val p = try{line match { case pattern(p1,p2,p3) => (p1,p2,p3)}} catch{case e : Throwable => null}
                if(p==null) System.exit(1)
                if(p._1==null || p._2==null) System.exit(1)
                (p._1.toInt,p._2.toDouble)
            })
            .collectAsMap()
        val w_britney = sc.broadcast(weights_britney)

        //scores a document based on its list of features
        def spamminess(features: Array[Int], modelName: String) : Double = {
            var score =0d
            if(modelName.equals("x"))
                features.foreach( f => if(w_x.value.contains(f)) score += w_x.value(f) )
            else if(modelName.equals("y"))
                features.foreach( f => if(w_y.value.contains(f)) score += w_y.value(f) )
            else if(modelName.equals("britney"))
                features.foreach( f => if(w_britney.value.contains(f)) score += w_britney.value(f) )
            score
        }
        
        val method: String = args.method()
        //get input from text file
        val textFile = sc.textFile(args.input())
        val test = textFile
            .map( line => {
                //Parse input
                val tokens = line.split(" ")
                val docId = tokens(0)
                val label = tokens(1)
                val features = tokens.slice(2,tokens.size).map( x => x.toInt )
                val score_x = spamminess(features,"x")
                val score_y = spamminess(features,"y")
                val score_britney = spamminess(features,"britney")
                var predict : String = "spam"
                var score: Double = 0.0
                if(method.equals("average")){
                    score = (score_x + score_y + score_britney)/3
                    predict = if( score > 0) "spam" else "ham"
                }
                else{
                    if(score_x>0) score=score+1 else score=score-1
                    if(score_y>0) score=score+1 else score=score-1
                    if(score_britney>0) score=score+1 else score=score-1
                    if(score>0) predict = "spam" else predict = "ham"
                }
                (docId,label,score,predict)
            })

            val fs = FileSystem.get(sc.hadoopConfiguration)
            val outputPath = new Path(args.output())
            if (fs.exists(outputPath))
                fs.delete(outputPath, true)
            test.saveAsTextFile(args.output())
    }
}
