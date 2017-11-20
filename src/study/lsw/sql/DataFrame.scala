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
      
    
    spark.stop()  
    
  }
  
}