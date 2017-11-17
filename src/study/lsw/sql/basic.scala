package study.lsw.sql

import org.apache.spark.sql.{DataFrame, SparkSession}

object basic {
  
  def main(args : Array[String]) {
    
    val saprk = SparkSession
      .builder()
      .appName("basic")
      .master("local[*]")
      .getOrCreate()
      
    val data = "./data"
    val df = saprk.read.text(data)
    
    runUntypedDFTransformation(df)
    
    // df.show()
    def runUntypedDFTransformation(df:DataFrame) : Unit = {
      import org.apache.spark.sql.functions._
      
      val wordDF = df.select(explode(split(col("value")," ")).as("word"))
      val result = wordDF.groupBy("word").count()
      result.show()
      result.printSchema()
    }
    
  }
  
}