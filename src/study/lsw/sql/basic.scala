package study.lsw.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object basic {
  
  def main(args : Array[String]) {
    
    val spark = SparkSession
      .builder()
      .appName("basic")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
      
    val data = "./data"
    val df = spark.read.text(data)
    
    //runUntypedDFTransformation(df)
    runTypedDSTransformation(spark,df)
  }
    
    // df.show()
    def runUntypedDFTransformation(df:DataFrame) : Unit = {
      import org.apache.spark.sql.functions._
      
      val wordDF = df.select(explode(split(col("value")," ")).as("word"))
      val result = wordDF.groupBy("word").count()
      result.show()
      result.printSchema()
    }
    
    def runTypedDSTransformation(spark: SparkSession,df:DataFrame) : Unit = {
      import spark.implicits._
      
      val ds = df.as[(String)]
      val result = ds.flatMap(_.split(" "))
                      .groupByKey(v => v)
                      .count()
       result.show()               
    }
  
}
  
