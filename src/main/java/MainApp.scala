import generator.DataGenerator
import generator.DataGenerator.{createJsonSchemaDF, createSimpleSchemaDF, readProductParquet, readSalesParquet, readSellersParquet}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import java.nio.file.Paths

object MainApp extends App {
  //TODO: 1) Montarme un HDFS
  //TODO: Hacer ejercicios desde aqui

  val conf = new SparkConf().setMaster("local").setAppName("MyBasicSpark").set("spark.executor.memory", "2g").set("spark.driver.memory", "2g")

  protected implicit lazy val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config(conf)
    .getOrCreate()

  val sqlContext = spark.sqlContext

  val productsDF: DataFrame = readProductParquet
  val salesDF: DataFrame = readSalesParquet
  val sellersDF = readSellersParquet

  productsDF.show(false)
  salesDF.show(false)
  sellersDF.show(false)

}

//Crear un esquema JSON y cargar algunos datos.
//val jsonDf = createJsonSchemaDF
//jsonDf.printSchema()
//jsonDf.show()

//Crear un esquema a mano y cargar algunos datos.
//val df = createSimpleSchemaDF
//df.printSchema()
//df.show()
