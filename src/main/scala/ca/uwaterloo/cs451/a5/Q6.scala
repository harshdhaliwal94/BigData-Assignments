package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession
import collection.mutable.ListBuffer

import collection.mutable.HashMap

class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "date", required = true)
    val text = opt[Boolean](descr = "text file", required = false, default = Some(false))
    val parquet = opt[Boolean](descr = "parquet file", required = false, default = Some(false))
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object Q6 extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    def main(argv : Array[String]){
        val args = new Q6Conf(argv)
        log.info("Input: "+args.input())
        log.info("Date: "+args.date())
        log.info("Text File: "+args.text())
        log.info("Parquet File: "+args.parquet())

        val conf = new SparkConf().setAppName("Q6")
        val sc = new SparkContext(conf)

        val date = args.date()
        val textFlag = args.text()
        val parquetFlag = args.parquet()
        if(textFlag){
            val item = sc.textFile(args.input() + "/lineitem.tbl")
                .filter(row => row.split("\\|")(10).contains(date))
                .map(row => {
                    val tokens = row.split("\\|")
                    val retflag = tokens(8)
                    val tax = tokens(7).toDouble
                    val extPrice = tokens(5).toDouble
                    val dscnt = tokens(6).toDouble
                    val lstatus = tokens(9)
                    val qnt = tokens(4).toLong
                    val dscntPrice = extPrice * (1 - dscnt)
                    val charge = dscntPrice * (1 - tax)
                    ((retflag, lstatus), (qnt, extPrice, dscntPrice, charge, dscnt, 1))
                })
                .reduceByKey((a, b) => (a._1+b._1, a._2+b._2, a._3+b._3, a._4+b._4, a._5+b._5, a._6+b._6))
                .collect()
                .foreach(x => {
                    val cnt = x._2._6
                    println(x._1._1, x._1._2, x._2._1, x._2._2, x._2._3, x._2._4, x._2._1/cnt, x._2._2/cnt, x._2._5/cnt, cnt)
                })
        } 
        else if(parquetFlag){
            val ss = SparkSession.builder.getOrCreate
            val lineDF = ss.read.parquet(args.input() + "/lineitem")
            val item = lineDF.rdd
                .filter(row => row.getString(10).contains(date))
                .map(row => {
                    val retflag = row.getString(8)
                    val tax = row.getDouble(7)
                    val extPrice = row.getDouble(5)
                    val dscnt = row.getDouble(6)
                    val lstatus = row.getString(9)
                    val qnt = row.getDouble(4).toInt
                    val dscntPrice = extPrice * (1 - dscnt)
                    val bill = dscntPrice * (1 - tax)
                    ((retflag, lstatus), (qnt, extPrice, dscntPrice, bill, dscnt, 1))
                })
                .reduceByKey((a, b) => (a._1+b._1, a._2+b._2, a._3+b._3, a._4+b._4, a._5+b._5, a._6+b._6))
                .collect()
                .foreach(x => {
                    val cnt = x._2._6
                    println(x._1._1, x._1._2, x._2._1, x._2._2, x._2._3, x._2._4, x._2._1/cnt, x._2._2/cnt, x._2._5/cnt, cnt)
                })
        }
    }
}
