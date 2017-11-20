package study.lsw.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._

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
    
    val sf1 = StructField("name", StringType, nullable = true)
    val sf2 = StructField("age",  IntegerType, nullable = true)
    val sf3 = StructField("job",  StringType, nullable = false)
    val schema = StructType(List(sf1, sf2, sf3))
    
    val df1 = spark.read.schema(schema).csv(sparkHomeDir+"/person.csv")
    df1.show()
  }
  
}