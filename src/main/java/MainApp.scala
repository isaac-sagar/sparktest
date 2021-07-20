import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object MainApp extends App {
  //TODO: 5) Meterlo en git (con gitignore)
  //TODO: 1) Añadir schemas json
  //TODO: 2) Generar datos como hacía Dani
  //TODO: 3) Meterle algunas operaciónes básicas
  //TODO: 4) Encapsular en Docker



  val conf = new SparkConf().setMaster("local").setAppName("MyBasicSpark")

  //val sc = new SparkContext(conf)
  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)

  val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config(conf)
    .getOrCreate()


    val simpleData = Seq(Row("James","","Smith","36636","M",3000),
      Row("Michael","Rose","","40288","M",4000),
      Row("Robert","","Williams","42114","M",4000),
      Row("Maria","Anne","Jones","39192","F",4000),
      Row("Jen","Mary","Brown","","F",-1)
    )

    val simpleSchema = StructType(Array(
      StructField("firstname",StringType,true),
      StructField("middlename",StringType,true),
      StructField("lastname",StringType,true),
      StructField("id", StringType, true),
      StructField("gender", StringType, true),
      StructField("salary", IntegerType, true)
    ))

    val df = spark.createDataFrame(spark.sparkContext.parallelize(simpleData),simpleSchema)

    df.printSchema()
    df.show()

}
