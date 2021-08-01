package generator

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

import scala.io.Source


object DataGenerator {

  def createSimpleSchemaDF(implicit spark: SparkSession) = {

    //Crear un esquema JSON y cargar algunos datos.
    //val jsonDf = createJsonSchemaDF
    //jsonDf.printSchema()
    //jsonDf.show()

    //Crear un esquema a mano y cargar algunos datos.
    //val df = createSimpleSchemaDF
    //df.printSchema()
    //df.show()

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

    spark.createDataFrame(spark.sparkContext.parallelize(simpleData), simpleSchema)

  }

  def createJsonSchemaDF(implicit spark: SparkSession) = {

    val arrayStructureData = Seq(
      Row(Row("James","","Smith"),"Cricket","female",10000),
      Row(Row("Michael","Rose",""),"Tennis","male",20000),
      Row(Row("Robert","","Williams"),"Cooking","male",30000),
      Row(Row("Maria","Anne","Jones"),null,"female",20000),
      Row(Row("Jen","Mary","Brown"),"Blogging","female",10000)
    )

    val url = getClass().getClassLoader().getResourceAsStream("schemas/person.json")
    val schemaSource = Source.fromInputStream(url).getLines.mkString
    val schemaFromJson = DataType.fromJson(schemaSource).asInstanceOf[StructType]

    spark.createDataFrame(spark.sparkContext.parallelize(arrayStructureData),schemaFromJson)

  }

  def readProductParquet(implicit spark: SparkSession) = {
    val productsDF = spark.read.load(
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00000-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00001-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00002-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00003-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00004-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00005-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00006-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00007-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00008-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00009-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00010-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00011-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00012-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00013-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00014-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00015-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/products_parquet/part-00016-0f978a44-491b-411b-9bf5-266bd9d4e836-c000.snappy.parquet"
    )
    productsDF
  }


  def readSalesParquet(implicit spark: SparkSession) = {
    val salesDF = spark.read.load(
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00001-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00002-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00003-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00004-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00005-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00006-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00007-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00008-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00009-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00010-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00011-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00012-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00013-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00014-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00015-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00016-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00017-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00018-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00019-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00020-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00021-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00022-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00023-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00024-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00025-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00026-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00027-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00028-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00029-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00030-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00031-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00032-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00033-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00034-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00035-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00036-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00037-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00038-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00039-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00040-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00041-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00042-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00043-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00044-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00045-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00046-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00047-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00048-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00049-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00050-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00051-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00052-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00053-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00054-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00055-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00056-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00057-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00058-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00059-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00060-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00061-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00062-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00063-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00064-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00065-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00066-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00067-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00068-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00069-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00070-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00071-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00072-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00073-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00074-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00075-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00076-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00077-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00078-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00079-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00080-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00081-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00082-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00083-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00084-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00085-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00086-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00087-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00088-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00089-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00090-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00091-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00092-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00093-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00094-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00095-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00096-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00097-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00098-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00099-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00100-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00101-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00102-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00103-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00104-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00105-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00106-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00107-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00108-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00109-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00110-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00111-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00112-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00113-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00114-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00115-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00116-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00117-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00118-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00119-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00120-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00121-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00122-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00123-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00124-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00125-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00126-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00127-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00128-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00129-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00130-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00131-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00132-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00133-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00134-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00135-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00136-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00137-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00138-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00139-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00140-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00141-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00142-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00143-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00144-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00145-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00146-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00147-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00148-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00149-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00150-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00151-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00152-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00153-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00154-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00155-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00156-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00157-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00158-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00159-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00160-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00161-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00162-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00163-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00164-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00165-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00166-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00167-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00168-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00169-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00170-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00171-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00172-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00173-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00174-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00175-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00176-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00177-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00178-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00179-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00180-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00181-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00182-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00183-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00184-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00185-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00186-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00187-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00188-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00189-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00190-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00191-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00192-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00193-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00194-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00195-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00196-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00197-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00198-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet",
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sales_parquet/part-00199-e651a798-938c-4056-96ac-b29d9c3b497e-c000.snappy.parquet"
    )
    salesDF
  }

  def readSellersParquet(implicit spark: SparkSession) = {
    val sellersDF = spark.read.load(
      "file:///C:/Users/Otros/IdeaProjects/sparktest/src/main/resources/data/sellers_parquet/part-00000-54f48a9e-d67c-4c29-aed7-1b5f6f117c12-c000.snappy.parquet"
    )
    sellersDF
  }

}
