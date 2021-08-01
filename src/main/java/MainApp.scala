import generator.DataGenerator
import generator.DataGenerator.{createJsonSchemaDF, createSimpleSchemaDF, readProductParquet, readSalesParquet, readSellersParquet}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, col, count, countDistinct, dense_rank, desc, first, last, nth_value, sum}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

import java.io.File
import java.nio.file.Paths

object MainApp extends App {
  //TODO: Hacer ejercicios desde aqui
  //TODO: Instalarme el IntelliJUltimate
  //TODO: Instalar el Big Data Tools
  //TODO: Intentar hacer el análisis de rendimiento del Exercise 1 desde BDT

  val conf = new SparkConf().setMaster("local").setAppName("MyBasicSpark").set("spark.executor.memory", "3gb").set("spark.sql.autoBroadcastJoinThreshold", "-1")

  protected implicit lazy val spark = SparkSession
    .builder()
    .appName("Spark SQL basic example")
    .config(conf)
    .getOrCreate()

  val sqlContext = spark.sqlContext

  val productsDF: DataFrame = readProductParquet
  val salesDF: DataFrame = readSalesParquet
  val sellersDF = readSellersParquet

  println("Sales: "+salesDF.schema)
  println("Prods: "+productsDF.schema)
  println("Sellers: "+sellersDF.schema)

  val t1 = System.nanoTime

  //println(sellersDF.schema)
  //Warmup 1-1
  //println("Product count: "+productsDF.count)
  //println("Sales count: "+salesDF.count)
  //println("Sellers count: "+sellersDF.count)

  //Warmup 1-2 Products sold at least once
  //println("Products sold at least once:"+salesDF.select("product_id").distinct().count)

  //Funciones de agregacion: org.apache.spark.sql
  //Si no usas groupby, considera todo el dataset como un solo grupo
  //salesDF.agg(countDistinct(col("product_id"))).show

  //Warmup 1-3 Top 20 most present products
  //salesDF.groupBy(col("product_id")).agg(count("*").alias("cnt")).orderBy(desc("cnt")).show

  //Warmup 2 Distinct products sold each day
  //salesDF.groupBy(col("date"))
  //  .agg(countDistinct("product_id").alias("cnt"))
  //  .orderBy(col("cnt").desc).show

  //Exercise 1: Order average revenue
  //salesDF.join(productsDF,salesDF("product_id")===productsDF("product_id"),"inner")
  //  .agg(avg(col("price")*col("num_pieces_sold")))
  //  .show()

  //salesDF.join(productsDF,salesDF("product_id")===productsDF("product_id"),"inner")
  //  .select((col("price")*col("num_pieces_sold")).alias("order_average"))
  //  .agg(avg(col("order_average")))
  //  .show()

  //Exercise 2:
  //Para cada orden, calcular el porcentaje de cuota diaria cumplida de la orden
  //salesDF.join(sellersDF,salesDF("seller_id")===sellersDF("seller_id"),"inner")
  //  .groupBy(salesDF("seller_id"))
  //  .agg(avg(col("num_pieces_sold")/col("daily_target"))*100)
  //  .show()

  //Exercise 3.0: First and last most successful seller for each product.

  //Mi sistema: Doble agregacion
  //val groupProdSellerSold = salesDF.groupBy(col("product_id"),col("seller_id"))
  //  .agg(sum("num_pieces_sold").alias("num_pieces_sold"))
  //  .orderBy(desc("num_pieces_sold"))
//
  //groupProdSellerSold.groupBy(col("product_id"))
  //  .agg(first(col("seller_id")), last(col("seller_id")))
  //  .filter(col("first(seller_id)") =!= col("last(seller_id)"))
  //  .orderBy(desc("product_id"))
  //  .show()


  //Agregación + Window:
  //val groupedSold = salesDF.groupBy(col("product_id"),col("seller_id"))
  //  .agg(sum("num_pieces_sold").alias("num_pieces_sold"))
//
  //val window_desc = Window.partitionBy(col("product_id")).orderBy(desc("num_pieces_sold"))
  //val window_asc = Window.partitionBy(col("product_id")).orderBy(asc("num_pieces_sold"))
//
  //val rankedGroupedSold = groupedSold
  //  .withColumn("rank_asc", dense_rank().over(window_asc))
  //  .withColumn("rank_desc", dense_rank().over(window_desc))
//
  //val bestSellersPerProduct = rankedGroupedSold.filter(col("rank_desc") === 1).withColumn("best_seller_id",col("seller_id"))
  //val worstSellersPerProduct = rankedGroupedSold.filter(col("rank_asc") === 1).withColumn("worst_seller_id",col("seller_id"))
//
  //bestSellersPerProduct.join(worstSellersPerProduct, bestSellersPerProduct("product_id")===worstSellersPerProduct("product_id"),"inner")
  //  .filter(col("best_seller_id") =!= col("worst_seller_id"))
  //  .orderBy(bestSellersPerProduct("product_id").desc).show()


//TODO: 3.1














  //Performance check
  println("Executed in: "+(System.nanoTime - t1) / 1e9d+" secs.")
}

//Crear un esquema JSON y cargar algunos datos.
//val jsonDf = createJsonSchemaDF
//jsonDf.printSchema()
//jsonDf.show()

//Crear un esquema a mano y cargar algunos datos.
//val df = createSimpleSchemaDF
//df.printSchema()
//df.show()
