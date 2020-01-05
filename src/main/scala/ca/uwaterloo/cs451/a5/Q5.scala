package ca.uwaterloo.cs451.a5

import io.bespin.scala.util.Tokenizer

import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.Partitioner
import org.rogach.scallop._
import org.apache.spark.sql.SparkSession

import collection.mutable.HashMap
import collection.mutable.ListBuffer

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val text = opt[Boolean](descr = "text file", required = false, default = Some(false))
    val parquet = opt[Boolean](descr = "parquet file", required = false, default = Some(false))
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object Q5 extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    def main(argv : Array[String]){
        val args = new Q5Conf(argv)
        log.info("Input: "+args.input())
        log.info("Text File: "+args.text())
        log.info("Parquet File: "+args.parquet())

        val conf = new SparkConf().setAppName("Q5")
        val sc = new SparkContext(conf)

        val textFlag = args.text()
        val parquetFlag = args.parquet()
        if(textFlag){
            val custMap = sc.textFile(args.input()+"/customer.tbl")
                .map(row => {(row.split("\\|")(0).toInt, row.split("\\|")(3).toInt)})
                .filter(x => (x._2 == 3 || x._2 == 24))
                .collectAsMap()
            val cust = sc.broadcast(custMap)
            
            val nationMap = sc.textFile(args.input()+"/nation.tbl")
                .map(row =>{(row.split("\\|")(0).toInt, row.split("\\|")(1))})
                .collectAsMap()
            val nation = sc.broadcast(nationMap)
            
            val order = sc.textFile(args.input()+"/orders.tbl")
                .map(row => {(row.split("\\|")(0).toInt, row.split("\\|")(1).toInt)})
            
            val item = sc.textFile(args.input()+"/lineitem.tbl")
                .map(row =>{
                    val oKey = row.split("\\|")(0).toInt
                    val date = row.split("\\|")(10)
                    val month = date.substring(0, date.lastIndexOf('-'))
                    (oKey,month)
                })
                .cogroup(order)
                .filter(_._2._1.size != 0)
                .flatMap(x =>{
                    var xList = ListBuffer[((String,String),Int)]()
                    if (cust.value.contains(x._2._2.head)) {
                        val nName = nation.value(cust.value(x._2._2.head))
                        val shipdates = x._2._1.iterator
                        while (shipdates.hasNext) {
                            xList += (((shipdates.next(), nName), 1))
                        }
                    }
                    xList
                })
                .reduceByKey(_ + _)
                .sortBy(_._1)
                .collect()
                .foreach(x => println(x._1._1, x._1._2, x._2))
        } 
        else if(parquetFlag){
            val ss = SparkSession.builder.getOrCreate
            val custDF = ss.read.parquet(args.input() + "/customer")
            val custMap = custDF.rdd
                .map(row => (row.getInt(0), row.getInt(3)))
                .filter(x => (x._2 == 3 || x._2 == 24))
                .collectAsMap()
            val cust = sc.broadcast(custMap)

            val nationDF = ss.read.parquet(args.input() + "/nation")
            val nationMap = nationDF.rdd
                .map(row => (row.getInt(0), row.getString(1)))
                .collectAsMap()
            val nation = sc.broadcast(nationMap)

            val ordersDF = ss.read.parquet(args.input() + "/orders")
            val order = ordersDF.rdd
                .map(row => {(row.getInt(0), row.getInt(1))})
            val lineDF = ss.read.parquet(args.input() + "/lineitem")
            val item = lineDF.rdd
                .map(row =>{
                    val oKey = row.getInt(0)
                    val date = row.getString(10)
                    val month = date.substring(0, date.lastIndexOf('-'))
                    (oKey,month)
                })
                .cogroup(order)
                .filter(_._2._1.size != 0)
                .flatMap(x =>{
                    var xList = ListBuffer[((String,String),Int)]()
                    if (cust.value.contains(x._2._2.head)) {
                        val nName = nation.value(cust.value(x._2._2.head))
                        val shipdates = x._2._1.iterator
                        while (shipdates.hasNext) {
                            xList += (((shipdates.next(), nName), 1))
                        }
                    }
                    xList
                })
                .reduceByKey(_ + _)
                .sortBy(_._1)
                .collect()
                .foreach(x => println(x._1._1, x._1._2, x._2))
        }
    }
}
