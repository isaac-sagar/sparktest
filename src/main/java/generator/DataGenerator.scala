package generator

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

import scala.io.Source


object DataGenerator {

  def createSimpleSchemaDF(implicit spark: SparkSession) = {

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

}
