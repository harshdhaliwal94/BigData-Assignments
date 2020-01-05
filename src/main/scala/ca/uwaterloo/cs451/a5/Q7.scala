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

class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "date", required = true)
    val text = opt[Boolean](descr = "text file", required = false, default = Some(false))
    val parquet = opt[Boolean](descr = "parquet file", required = false, default = Some(false))
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object Q7 extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    def main(argv : Array[String]){
        val args = new Q7Conf(argv)
        log.info("Input: "+args.input())
        log.info("Date: "+args.date())
        log.info("Text File: "+args.text())
        log.info("Parquet File: "+args.parquet())

        val conf = new SparkConf().setAppName("Q7")
        val sc = new SparkContext(conf)

        val date = args.date()
        val textFlag = args.text()
        val parquetFlag = args.parquet()
        if(textFlag){
            val custMap =  sc.textFile(args.input() + "/customer.tbl")
                .map( row => (row.split("\\|")(0).toInt,row.split("\\|")(1)))
                .collectAsMap()
            val cust = sc.broadcast(custMap)

            val order = sc.textFile(args.input() + "/orders.tbl")
                .filter(row =>(
                    cust.value.contains(row.split("\\|")(1).toInt) && row.split("\\|")(4) < date
                ))
                .map(row => {
                    val tokens = row.split("\\|")
                    val oKey = tokens(0).toInt
                    val cName = cust.value(tokens(1).toInt)
                    val oDate = tokens(4)
                    val priority = tokens(5)
                    (oKey, (cName, oDate, priority))
                })
                val item = sc.textFile(args.input() + "/lineitem.tbl")
                    .filter(row => row.split("\\|")(10) > date)
                    .map(row => {
                        val tokens = row.split("\\|")
                        val revenue = tokens(5).toDouble*(1-tokens(6).toDouble)
                        (tokens(0).toInt, revenue)
                    })
                    .reduceByKey(_ + _)
                    .cogroup(order)
                    .filter(x => x._2._1.size!= 0 && x._2._2.size!= 0)
                    .map(t => {
                        val cName = t._2._2.head._1
                        val oDate = t._2._2.head._2
                        val priority = t._2._2.head._3
                        val oKey = t._1
                        val revenue = t._2._1.foldLeft(0.0)((x, y) => x + y)
                        (revenue, (cName, oKey, oDate, priority))
                    })
                    .sortByKey(false)
                    .collect()
                    .take(10)
                    .foreach(a => println(a._2._1, a._2._2, a._1, a._2._3, a._2._4))
        } 
        else if(parquetFlag){
            val ss = SparkSession.builder.getOrCreate
            val custDF = ss.read.parquet(args.input() + "/customer")
            val custMap =  custDF.rdd
                .map( row => (row.getInt(0),row.getString(1)))
                .collectAsMap()
            val cust = sc.broadcast(custMap)

            val orderDF = ss.read.parquet(args.input() + "/orders")
            val order = orderDF.rdd
                .filter(row =>(
                    cust.value.contains(row.getInt(1)) && row.getString(4) < date
                ))
                .map(row => {
                    val oKey = row.getInt(0)
                    val cName = cust.value(row.getInt(1))
                    val oDate = row.getString(4)
                    val priority = row.getString(5)
                    (oKey, (cName, oDate, priority))
                })
                
                val lineitemDF = ss.read.parquet(args.input() + "/lineitem")
                val item = lineitemDF.rdd
                    .filter(row => row.getString(10) > date)
                    .map(row => {
                        val revenue = row.getDouble(5)*(1-row.getDouble(6))
                        (row.getInt(0), revenue)
                    })
                    .reduceByKey(_ + _)
                    .cogroup(order)
                    .filter(x => x._2._1.size!= 0 && x._2._2.size!= 0)
                    .map(t => {
                        val cName = t._2._2.head._1
                        val oDate = t._2._2.head._2
                        val priority = t._2._2.head._3
                        val oKey = t._1
                        val revenue = t._2._1.foldLeft(0.0)((x, y) => x + y)
                        (revenue, (cName, oKey, oDate, priority))
                    })
                    .sortByKey(false)
                    .collect()
                    .take(10)
                    .foreach(a => println(a._2._1, a._2._2, a._1, a._2._3, a._2._4))
        }
    }
}