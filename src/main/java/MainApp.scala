import generator.DataGenerator.{createJsonSchemaDF, createSimpleSchemaDF}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MainApp extends App {
  //TODO: 1) Añadir schemas json
  //TODO: 2) Generar datos como hacía Dani
  //TODO: 3) Meterle algunas operaciónes básicas
  //TODO: 4) Encapsular en Docker

  val conf = new SparkConf().setMaster("local").setAppName("MyBasicSpark")

  //val sc = new SparkContext(conf)
  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  protected implicit lazy val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config(conf)
    .getOrCreate()

  val df = createSimpleSchemaDF
  df.printSchema()
  df.show()

  //val jsonDf = createJsonSchemaDF
  //jsonDf.printSchema()
  //jsonDf.show()

}
