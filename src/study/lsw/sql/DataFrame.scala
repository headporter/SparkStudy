package study.lsw.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame

object DataFrame {
  def main (args:Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("DF test")
      .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
      
    createDF(spark)
    spark.stop()  
    }
  
  def createDF(spark:SparkSession) {
    import spark.implicits._
    
    val sparkHomeDir = "./data"
    val df = spark.read.csv(sparkHomeDir+"/person.csv")
    
    df.show()
  }
  
}