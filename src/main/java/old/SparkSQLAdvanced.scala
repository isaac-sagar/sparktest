package old

import functions.Encoder.toUpper
import generator.DataGenerator
import generator.DataGenerator.{createJsonSchemaDF, createSimpleSchemaDF, readProductParquet, readSalesParquet, readSellersParquet}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{asc, avg, col, count, countDistinct, dense_rank, desc, first, last, nth_value, sum}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object SparkSQLAdvanced {

  def exerciseCall  (implicit spark: SparkSession) = {

    val productsDF: DataFrame = readProductParquet
    val salesDF: DataFrame = readSalesParquet
    val sellersDF = readSellersParquet

    println("Sales: "+salesDF.schema)
    println("Prods: "+productsDF.schema)
    println("Sellers: "+sellersDF.schema)

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


    //3.1 Send the SECOND best. NULL if there isn't any
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
    //val bestSellersPerProduct = rankedGroupedSold.filter(col("rank_desc") === 2).withColumn("second_best_seller_id",col("seller_id"))
    //val worstSellersPerProduct = rankedGroupedSold.filter(col("rank_asc") === 1).withColumn("worst_seller_id",col("seller_id"))
    //
    //worstSellersPerProduct.join(bestSellersPerProduct, bestSellersPerProduct("product_id")===worstSellersPerProduct("product_id"),"left")
    //  .filter(col("second_best_seller_id").isNotNull && col("second_best_seller_id") =!= col("worst_seller_id"))
    //  .orderBy(worstSellersPerProduct("product_id").desc).show()

    // Exercise 4: Nueva fila con transformaciones no-directas dependiendo del contenido de otra fila/s
    import org.apache.spark.sql.functions.udf
    val upper = udf(toUpper)

    val result = salesDF.withColumn("hashed_bill", upper(col("order_id"),col("bill_raw_text")))

    result.cache()
    result.show()
    //println(result.select("hashed_bill").count() == result.select("hashed_bill").distinct().count())

  }

}
