package old

import org.apache.spark.sql.DataFrame
import org.apache.spark.{SparkConf, SparkContext}

object HelloWorld extends App {
  val conf = new SparkConf().setMaster(args(0)).setAppName("MyFirstSpark")
  val sc = new SparkContext(conf)

  sc.setLogLevel("ERROR")

  //println("Hello, world!")
  //val dataNum = (1 to 100).toList
  //val rddDataNum = sc.parallelize(dataNum)
  //println(rddDataNum.count())

  //rddDataNum.cache().take(10).foreach(println(_))
  //rddDataNum.foreach(println(_))

  //val dataText = sc.textFile("src/main/resources/quijote.txt")
  //println(dataText.count())

  //rddDataNum
    //.map(_*2)
    //.foreach(i => println(i))

  //rddDataNum
  // .map(i => (i%10, 1))
  // .reduceByKey((a,b) => a+b)
  // .foreach(println(_))
  //reduceByKey + funcion, en caso de coincidir las keys, agrupar usando la funcion sobre los valores.

  //val quixote = sc.textFile("src/main/resources/quijote.txt")
  //data.cache()

  //Ejercicio 4
  //data.flatMap(line => line.split(" "))    .filter(i => !i.contains("Sancho") && !i.contains("que"))    .map(word => (word, 1))    .reduceByKey((a,b) => a+b)    .sortBy(_._2,false)    .take(10)    .foreach(i => println(i._1))

  //val pairs = sc.parallelize(Array(("a", 3), ("a", 1), ("b", 7), ("a", 5)))

  //4.2
  //pairs.groupByKey().foreach(println(_)) Agrupa directamente por key
  //pairs.aggregateByKey(new HashSet[Int])((a,b) => a+b,(a,b) => a++b).foreach(println(_)) Agrupa por key, pero siguiento directrices: p1: Tipo de agrupacion, p2: set+elemento, p3: set+set

  //val A = sc.parallelize((0 to 100).toList)
  //val B = sc.parallelize((0 to 1000 by 10).toList)

  //A.union(B).distinct().collect.sortWith(_<_).foreach(println(_))
  //A.intersection(B).collect.sortWith(_<_).foreach(println(_))

  //val users = sc.parallelize(List((1,"John"),(2,"Mary"),(3,"Mark"))) // (UserId,Name) pairs
  //val accounts = sc.parallelize(List((1,("#1",1000)),(1,("#2",500)),(2,("#3",750))))
  //users.cogroup(accounts).foreach(println)

  //Ex 12
  //val wordLengths = quixote.flatMap(line => line.split(" ")).filter(word => word.length > 4 && !List("Quixote", "Sancho").contains(word)).map(_.length).cache

  //val accum = sc.doubleAccumulator("AccumLength")
  //wordLengths.foreach(length => accum.add(length))

  //val noWords = wordLengths.count()

  //println(accum.sum/noWords)

  //val sqlContext = new org.apache.spark.sql.SQLContext(sc)
  //val df:DataFrame = sqlContext.read.format("com.databricks.spark.csv").option("header", "true").load("src/main/resources/olympicathletes.csv")

  //df.registerTempTable("Records")

  //df.select("Athlete", "Gold").where("Gold>0").groupBy("Athlete").agg(sum("Gold")).foreach(println(_))
  //sqlContext.sql("select athlete, sum(gold) from Records group by athlete;").foreach(println(_))
  //sqlContext.udf.register("strLen", (s: String) => s.length())
  //sqlContext.sql("select distinct Country, strLen(Country) from Records;").foreach(println(_))

  val pathToFile = "src/main/resources/reducedtweets.json"
  val sqlcontext = new org.apache.spark.sql.SQLContext(sc)

  //loadData()
  //showDataFrame()
  printSchema()
  println(mostPopularTwitterer)

  /**
   *  Here the method to create the contexts (Spark and SQL) and
   *  then create the dataframe.
   */
  def loadData(): DataFrame = {
    // Load the data regarding the file is a json file
    // Hint: use the sqlContext and apply the read method before loading the json file
    val df:DataFrame = sqlcontext.read.json(pathToFile)
    df
  }


  /**
   *  See how looks the dataframe
   */
  def showDataFrame() = {
    val dataframe = loadData()
    dataframe.foreach(println(_))
  }

  /**
   * Print the schema
   */
  def printSchema() = {
    val dataframe = loadData()
    dataframe.printSchema()
  }

  /**
   * Find people who are located in Paris
   */
  def filterByLocation(): DataFrame = {
    val dataframe = loadData()
    dataframe.registerTempTable("Jsons")
    sqlcontext.sql("select distinct user from Jsons where place=='Paris';")
  }


  /**
   *  Find the user who tweets the more
   */
  def mostPopularTwitterer(): (Long, String) = {
    val dataframe = loadData()
    dataframe.registerTempTable("Jsons")
    val topTwitter = sqlcontext.sql("select user, count(1) amount from Jsons group by user order by amount DESC LIMIT 1;").first()
    (topTwitter.getLong(1),topTwitter.getString(0))
  }





}
