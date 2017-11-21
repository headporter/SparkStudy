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
      
    val DFwithSchema = createDF(spark)
    runBasicOpsEx(DFwithSchema)
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
    
    val DFwithSchema = spark.read.schema(schema).csv(sparkHomeDir+"/person.csv")
   // DFwithSchema.show()
    return DFwithSchema
  }
  
  def runBasicOpsEx(df : DataFrame) {
    df.show()
    //df.head()
  }
  
}