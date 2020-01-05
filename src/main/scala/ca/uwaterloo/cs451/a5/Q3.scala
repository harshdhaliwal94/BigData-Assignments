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

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
    mainOptions = Seq(input, date, text, parquet)
    val input = opt[String](descr = "input path", required = true)
    val date = opt[String](descr = "date", required = true)
    val text = opt[Boolean](descr = "text file", required = false, default = Some(false))
    val parquet = opt[Boolean](descr = "parquet file", required = false, default = Some(false))
    val numExecutors = opt[Int](descr = "number of executors", required = false, default = Some(1))
    val executorCores = opt[Int](descr = "number of cores", required = false, default = Some(1))
    verify()
}

object Q3 extends Tokenizer{
    val log = Logger.getLogger(getClass().getName())
    def main(argv : Array[String]){
        val args = new Q3Conf(argv)
        log.info("Input: "+args.input())
        log.info("Date: "+args.date())
        log.info("Text File: "+args.text())
        log.info("Parquet File: "+args.parquet())

        val conf = new SparkConf().setAppName("Q3")
        val sc = new SparkContext(conf)

        val date = args.date()
        val textFlag = args.text()
        val parquetFlag = args.parquet()
        if(textFlag){
            val partMap = sc.textFile(args.input()+"/part.tbl")
                .map(row => {(row.split("\\|")(0).toInt, row.split("\\|")(1))})
                .collectAsMap()
            val part = sc.broadcast(partMap)
            val supplierMap = sc.textFile(args.input()+"/supplier.tbl")
                .map(row =>{(row.split("\\|")(0).toInt, row.split("\\|")(1))})
                .collectAsMap()
            val supplier = sc.broadcast(supplierMap)
            val item = sc.textFile(args.input()+"/lineitem.tbl")
                .filter(row => {row.split("\\|")(10).contains(date)})
                .map(row => {
                    val tokens = row.split("\\|")
                    val oKey = tokens(0).toInt
                    val lPartKey = tokens(1).toInt
                    val lSuppKey = tokens(2).toInt
                    val pName = if(part.value.keys.exists(_==lPartKey)) part.value(lPartKey) else None
                    val sName = if(supplier.value.keys.exists(_==lSuppKey)) supplier.value(lSuppKey) else None
                    (oKey,(pName,sName))
                })
                .filter( x => (x._2._1!=None && x._2._2!=None))
                .sortByKey()
                .take(20)
                .foreach(x => println(x._1, x._2._1, x._2._2))
        } 
        else if(parquetFlag){
            val ss = SparkSession.builder.getOrCreate
            val partTblDf = ss.read.parquet(args.input()+"/part")
            val partMap = partTblDf.rdd
                .map(row => {(row.getInt(0), row.getString(1))})
                .collectAsMap()
            val part = sc.broadcast(partMap)
            val suppTblDf = ss.read.parquet(args.input()+"/supplier")
            val supplierMap = suppTblDf.rdd
                .map(row =>{(row.getInt(0), row.getString(1))})
                .collectAsMap()
            val supplier = sc.broadcast(supplierMap)
            val lineTblDf = ss.read.parquet(args.input()+"/lineitem")
            val item = lineTblDf.rdd
                .filter(row => {row.getString(10).contains(date)})
                .map(row =>{
                    val oKey = row.getInt(0)
                    val lPartKey = row.getInt(1)
                    val lSuppKey = row.getInt(2)
                    val pName = if(part.value.keys.exists(_==lPartKey)) part.value(lPartKey) else None
                    val sName = if(supplier.value.keys.exists(_==lSuppKey)) supplier.value(lSuppKey) else None
                    (oKey,(pName,sName))
                })
                .filter( x => (x._2._1!=None && x._2._2!=None))
                .sortByKey()
                .take(20)
                .foreach(x => println(x._1, x._2._1, x._2._2))
        }
    }
}
