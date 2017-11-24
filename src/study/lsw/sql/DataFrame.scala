package study.lsw.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object DataFrame {
  def main(args: Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("DF test")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext
    val dfWithSchema = createDF(spark)
    //runBasicOpsEx(dfWithSchema,spark)
    //runColEx(dfWithSchema,spark)
    //runIsinEx(dfWithSchema,spark,sc)
    //runWhenEx(dfWithSchema)
    //runAggregationEx(dfWithSchema)
    //runDateFunctions(spark)
    //runOrderEx(spark,dfWithSchema)
    //runUDF(spark,dfWithSchema)
    //runDistinct(spark)
    //runNull(dfWithSchema)
    runWithCol(dfWithSchema)
    spark.stop()
  }

  def createDF(spark: SparkSession): DataFrame = {
    import spark.implicits._

    val sparkHomeDir = "./data"
    val df = spark.read.csv(sparkHomeDir + "/person.csv")

    //df.show()

    val sf1 = StructField("name", StringType, nullable = true)
    val sf2 = StructField("age", IntegerType, nullable = true)
    val sf3 = StructField("job", StringType, nullable = false)
    val schema = StructType(List(sf1, sf2, sf3))

    spark.read.schema(schema).csv(sparkHomeDir + "/person.csv")
    
    
  }

  def runBasicOpsEx(df: DataFrame, spark: SparkSession) {
    df.show()
    println("head(3) : ", df.head(3))
    println(df.count())
    println(df.first())
    println(df.take(2))
    df.describe("age").show
    df.persist(StorageLevel.MEMORY_AND_DISK_2)
    df.printSchema()
    df.columns
    df.dtypes
    df.schema
    df.createOrReplaceTempView("users")
    spark.sql("select name, age from users where age > 20").show
    spark.sql("select name, age from users where age > 20").explain
  }

  def runColEx(df: DataFrame, spark: SparkSession) {
    import spark.implicits._

    df.where(df("age") > 10).show()
    df.where('age > 20).show()
    df.select(('age + 1).as("changeAge")).where($"age" > 30).show()
    df.where(df("name").contains("ng")).show
    df.where(df("age") between (20, 30)).show
    // df.where(col("age")>10).show()

  }

  def runIsinEx(df: DataFrame, spark: SparkSession, sc: SparkContext) {
    import spark.implicits._

    val inJobs = sc.broadcast(List("teacher", "student"))
    df.where(df("job").isin(inJobs.value: _*)).show()

    df.filter($"job".isin("teacher", "student")).show()
    df.filter(!$"job".isin("teacher", "student")).show()
    // read meta file doesn't work
    val inJobsCsv = spark.read.csv("./data/job_meta.csv") //.select("_c0").collect().toSeq
    //df.filter(df("job")isin(inJobsCsv : _*)).show()

    val tmpInJobsCsv = inJobsCsv
    df.join(
      broadcast(inJobsCsv),
      df("job") === inJobsCsv("_c0"), "left").show()

    df.join(
      inJobsCsv,
      df("job") === inJobsCsv("_c0"), "inner").show()
  }

  //http://sqlandhadoop.com/spark-datafarme-when-case/
  def runWhenEx(df: DataFrame) {
    val ageClass = when(df("age") > 20, "old").otherwise("young").as("type")
    df.select(df("name"), df("age"), ageClass).show()

    df.withColumn(
      "result",
      when(col("job") === ("teacher"), "great")
        .when(col("job") === ("student"), "soso")
        .otherwise("???")).show()

  }

  def runAggregationEx(df: DataFrame) {
    df.select(min(df("age")), max(df("age")), sum(df("age"))).show()

    val doubleDF = df.union(df)
    doubleDF.select(collect_list("name")).show()
    doubleDF.select(collect_set("name")).show()
    doubleDF.select(count("name"), countDistinct("name")).show()
    doubleDF.cube("job").agg(sum("age"), grouping("job")).show
    doubleDF.groupBy("name", "job").agg("age" -> "sum").show()
  }

  def runDateFunctions(spark: SparkSession) {
    import org.apache.spark.sql.functions._
    import spark.implicits._
    val dateTime = "2017-12-15 12:00:01"
    val date = "2017-12-15"
    
    println(System.currentTimeMillis())
       
    val df = Seq((dateTime, date)).toDF("a", "b")
    df.show()

    val cuTime = current_date().as("CT")
    val unixTime = unix_timestamp(df("a")).as("uxt")
    val toDate = to_date(df("a")).as("to_date")
    val unixToDate = to_date(unixTime.cast("timestamp")).as("ux_to_date")
    df.select(df("a"), df("b"), cuTime,unixTime,toDate,unixToDate).show()
    
    val addMonth = add_months(df("a"),2).as("addMon")
    val addDate = date_add(df("a"),3).as("addDt")
    val lastDay = last_day(df("a")).as("lastday")
    df.select(df("a"), addMonth, addDate, lastDay).show()
    
    // window
    val p1 = ("2017-12-25 12:01:00", "note", 1000)
    val p2 = ("2017-12-25 12:01:10", "pencil", 3500)
    val p3 = ("2017-12-25 12:03:20", "pencil", 23000)
    val p4 = ("2017-12-25 12:05:00", "note", 1500)
    val p5 = ("2017-12-25 12:05:07", "note", 2000)
    val p6 = ("2017-12-25 12:06:25", "note", 1000)
    val p7 = ("2017-12-25 12:08:00", "pencil", 500)
    val p8 = ("2017-12-25 12:09:45", "note", 30000)

    val dd = Seq(p1, p2, p3, p4, p5, p6, p7, p8).toDF("date", "product", "amount")
    dd.groupBy(window(unix_timestamp('date).cast("timestamp"), "5 minutes"), 'product).agg(sum('amount)).show(false)
  }

  def runOrderEx (spark:SparkSession, df:DataFrame) {
    import spark.implicits._
    
    val p1 = ("2017-12-25 12:01:00", "note", 1000)
    val p2 = ("2017-12-25 12:01:10", "pencil", 3500)
    val p3 = ("2017-12-25 12:03:20", "pencil", 23000)
    val p4 = ("2017-12-25 12:05:00", "note", 1500)
    val p5 = ("2017-12-25 12:05:07", "note", 2000)
    val p6 = ("2017-12-25 12:06:25", "note", 1000)
    val p7 = ("2017-12-25 12:08:00", "pencil", 500)
    val p8 = ("2017-12-25 12:09:45", "note", 30000)

    val dd = Seq(p1, p2, p3, p4, p5, p6, p7, p8).toDF("date", "product", "amount")
        
    val w1 = Window.partitionBy("product").orderBy("amount")
    val w2 = Window.orderBy("amount")
    dd.select('date,'product, 'amount, row_number().over(w1).as("rownum"), rank().over(w2).as("rank")).show
    
    df.sort(desc("age"),asc("name")).show()
    // desc_nulls_first, desc_nulls_last, asc_nulls_first, asc_nulls_last
    df.sort(asc_nulls_first("job")).show()
  }
  
   def runUDF (spark : SparkSession, df : DataFrame) {
     import spark.implicits._
     
     // use function
     val caseWhen = udf((job : String) => job match {
       case "student" => "poor"
       case "teacher" => "better"  
       case _ => "unknown"  
         
     })
     df.select('name,'job,caseWhen('job)).show()
   }
   
 def runDistinct(spark: SparkSession) {
    import spark.implicits._
    val d1 = ("store1", "note", 20, 2000)
    val d2 = ("store1", "bag", 10, 5000)
    val d3 = ("store1", "note", 20, 2000)
    val d4 = ("store1", "note", 20, 2000)
    val df = Seq(d1, d2, d3, d4).toDF("store", "product", "amount", "price")
    df.distinct.show
    df.dropDuplicates("store").show
    df.dropDuplicates().show
  }
 
  def runIntersect(spark: SparkSession) {
    val a = spark.range(1, 5).toDF
    val b = spark.range(2, 6).toDF
    val c = a.intersect(b)
    c.show
  }

  def runExcept(spark: SparkSession) {
    import spark.implicits._
    val df1 = List(1, 2, 3, 4, 5).toDF
    val df2 = List(2, 4).toDF
    df1.except(df2).show
  }

  def runNull (df : DataFrame) {
    df.na.drop().show()
    df.na.fill(Map("job" -> "unknown")).show
    df.na.replace("job",Map("teacher" -> "tt", "student" -> "ss" )).show
  }
  
  def runWithCol (df : DataFrame) {
    df.withColumn("addAge", df("age")+10).show
    df.withColumnRenamed("name", "famillyname").show
  }
}