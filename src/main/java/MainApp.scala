import generator.DataGenerator.{createJsonSchemaDF, createSimpleSchemaDF}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

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

  val jsonDf = createJsonSchemaDF
  jsonDf.printSchema()
  jsonDf.show()

}

//Crear un esquema JSON y cargar algunos datos.
//val jsonDf = createJsonSchemaDF
//jsonDf.printSchema()
//jsonDf.show()

//Crear un esquema a mano y cargar algunos datos.
//val df = createSimpleSchemaDF
//df.printSchema()
//df.show()
