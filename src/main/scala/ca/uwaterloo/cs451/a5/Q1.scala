package ca.uwaterloo.cs451.a5

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "date", required = true)
    val text = opt[Boolean](descr = "text file", required = false, default = Some(false))
    val parquet = opt[Boolean](descr = "parquet file", required = false, default = Some(false))
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object Q1 {
    val log = Logger.getLogger(getClass().getName())
    def main(argv: Array[String]){
        val args = new Q1Conf(argv)
        log.info("Input: "+args.input())
        log.info("Date: "+args.date())
        log.info("Text File: "+args.text())
        log.info("Parquet File: "+args.parquet())
        //get input from text file

        val conf = new SparkConf().setAppName("Q1")
        val sc = new SparkContext(conf)

        val date = args.date()
        val textFlag = args.text()
        val parquetFlag = args.parquet()

        if(textFlag){
            val textFile = sc.textFile(args.input()+"/lineitem.tbl")
            val answer = textFile
                .map(row => row.split("\\|")(10))
                .filter(shipdate => shipdate.contains(date))
                .count()
            println("ANSWER="+answer)
        }else if(parquetFlag){
            val ss = SparkSession.builder().getOrCreate()
            val lineitemTblDf = ss.read.parquet(args.input()+"/lineitem")
            val lineitemTblRDD = lineitemTblDf.rdd
            val answer = lineitemTblRDD
                .map(row => row.getString(10))
                .filter(shipdate => shipdate.contains(date))
                .count()
            println("ANSWER="+answer)
        }
    }
}