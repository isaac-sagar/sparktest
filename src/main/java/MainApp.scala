import generator.DataGenerator.{createJsonSchemaDF, createSimpleSchemaDF}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MainApp extends App {
  //1) Añadir schemas json
  //2) Generar datos como hacía Dani
  //TODO: 3) Meterle algunas operaciónes básicas
  //4) Encapsular en Docker
  //docker build --tag sparktest:latest .
  //docker run -d --memory=6g --name sparktesting -p 9080:9080 sparktest:latest
  //TODO: 5) Añadir Readme.MD

  val conf = new SparkConf().setMaster("local").setAppName("MyBasicSpark").set("spark.executor.memory", "2g").set("spark.driver.memory", "2g")

  //val sc = new SparkContext(conf)
  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  protected implicit lazy val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config(conf)
    .getOrCreate()

  //val df = createSimpleSchemaDF
  //df.printSchema()
  //df.show()

  val jsonDf = createJsonSchemaDF
  jsonDf.printSchema()
  jsonDf.show()

}
