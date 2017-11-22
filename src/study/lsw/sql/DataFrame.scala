package study.lsw.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
//import org.apache.spark.broadcast

object DataFrame {
  def main (args:Array[String]) = {
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
    runWhenEx(dfWithSchema)
    
    
    
    spark.stop()  
    }
  
  def createDF(spark:SparkSession) : DataFrame = {
    import spark.implicits._
    
    val sparkHomeDir = "./data"
    val df = spark.read.csv(sparkHomeDir+"/person.csv")
    
    //df.show()
    
    val sf1 = StructField("name", StringType, nullable = true)
    val sf2 = StructField("age",  IntegerType, nullable = true)
    val sf3 = StructField("job",  StringType, nullable = false)
    val schema = StructType(List(sf1, sf2, sf3))
    
    spark.read.schema(schema).csv(sparkHomeDir+"/person.csv")
  }
  
  def runBasicOpsEx(df : DataFrame, spark : SparkSession) {
    df.show()
    println("head(3) : ",df.head(3))
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
  
  def runColEx(df : DataFrame, spark : SparkSession) {
    import spark.implicits._
    
    df.where(df("age") > 10).show()
    df.where('age>20).show()
    df.select(('age+1).as("changeAge")).where($"age">30).show()
    df.where(df("name").contains("ng")).show
    df.where(df("age")between(20,30)).show
   // df.where(col("age")>10).show()
    
  }
  
  def runIsinEx(df : DataFrame, spark : SparkSession, sc: SparkContext) {
    import spark.implicits._
   
    
    val inJobs = sc.broadcast(List("teacher","student"))
    df.where(df("job").isin(inJobs.value : _*)).show()
    
    df.filter( $"job" .isin ("teacher","student")).show()
    df.filter( !$"job" .isin ("teacher","student")).show()
    // read meta file doesn't work
    val inJobsCsv = spark.read.csv("./data/job_meta.csv")//.select("_c0").collect().toSeq
    //df.filter(df("job")isin(inJobsCsv : _*)).show()
    
    val tmpInJobsCsv = inJobsCsv
    df.join(broadcast(inJobsCsv),
        df("job") === inJobsCsv("_c0")
        ,"left").show()
        
         df.join(inJobsCsv,
        df("job") === inJobsCsv("_c0")
        ,"inner").show()   
  }
  
 def runWhenEx (df:DataFrame) {
   val ageClass = when(df("age")>20, "old").otherwise("young").as("type")
   df.select(df("name"),df("age"),ageClass).show()
 }
  
  
  
  
  
}