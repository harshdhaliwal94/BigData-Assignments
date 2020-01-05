package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "date", required = true)
    val text = opt[Boolean](descr = "text file", required = false, default = Some(false))
    val parquet = opt[Boolean](descr = "parquet file", required = false, default = Some(false))
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object Q2 {
    val log = Logger.getLogger(getClass().getName())
    def main(argv: Array[String]){
        val args = new Q2Conf(argv)
        log.info("Input: "+args.input())
        log.info("Date: "+args.date())
        log.info("Text File: "+args.text())
        log.info("Parquet File: "+args.parquet())

        val conf = new SparkConf().setAppName("Q2")
        val sc = new SparkContext(conf)

        val date = args.date()
        val textFlag = args.text()
        val parquetFlag = args.parquet()

        if(textFlag){
            val items = sc.textFile(args.input() + "/lineitem.tbl")
                .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(10)))
                .filter(x => {x._2.contains(date)})

            val clerks = sc.textFile(args.input() + "/orders.tbl")
                .map(line => (line.split("\\|")(0).toInt, line.split("\\|")(6)))
                .cogroup(items)
                .filter(x => {x._2._2.size != 0})
                .sortByKey()
                .take(20)
                .map(x => {(x._2._1.head, x._1.toLong)})
                .foreach(println)

        }else if(parquetFlag){
            val ss = SparkSession.builder.getOrCreate
            val lineitemTblDf = ss.read.parquet(args.input()+"/lineitem")
            val lineitemRDD = lineitemTblDf.rdd
            val ordersTblDf = ss.read.parquet(args.input()+"/orders")
            val ordersRDD = ordersTblDf.rdd
            val items = lineitemRDD
                .map(row =>{(row.getInt(0),row.getString(10))})
                .filter(x => x._2.contains(date))

            val clerks = ordersRDD
                .map(row =>{(row.getInt(0), row.getString(6))})
                .cogroup(items)
                .filter(x => {x._2._2.size != 0})
                .sortByKey()
                .take(20)
		.map(x =>{(x._2._1.head, x._1.toLong)})
                .foreach(println)
        }
    }

}
