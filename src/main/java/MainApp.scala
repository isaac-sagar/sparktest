import functions.Encoder.toUpper
import generator.DataGenerator
import old.SparkSQLAdvanced
import generator.DataGenerator.{createJsonSchemaDF, createSimpleSchemaDF, readProductParquet, readSalesParquet, readSellersParquet}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, col, count, countDistinct, dense_rank, desc, first, last, nth_value, sum}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import java.math.BigInteger
import java.nio.file.Paths
import java.security.MessageDigest
import javax.xml.bind.annotation.adapters.HexBinaryAdapter

object MainApp extends App {
  //TODO: Instalarme el IntelliJUltimate
  //TODO: Instalar el Big Data Tools
  //TODO: Intentar hacer el análisis de rendimiento del Exercise 1 desde BDT

  val conf = new SparkConf().setMaster("local").setAppName("MyBasicSpark").set("spark.executor.memory", "3gb").set("spark.sql.autoBroadcastJoinThreshold", "-1")

  protected implicit lazy val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config(conf)
    .getOrCreate()

  //val sqlContext = spark.sqlContext
  val t1 = System.nanoTime

  SparkSQLAdvanced.exerciseCall

  //Performance check
  println("Executed in: "+(System.nanoTime - t1) / 1e9d+" secs.")
}


