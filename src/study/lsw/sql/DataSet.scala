package study.lsw.sql

import org.apache.spark.sql.{Dataset, SparkSession,DataFrame}
import org.apache.spark.sql.functions._

object DataSet {
  def main (args : Array[String]) {
    val spark = SparkSession
          .builder()
          .appName("DS test")
          .master("local[*]")
          .config("spark.driver.host","127.0.0.1")
          .getOrCreate()
          
     //val ds = createDS(spark)     
     //ds.show()   
     //ds.printSchema()
     //runSelectEx(spark,ds)
     //createMissingDS(spark)
     //ds1.printSchema()
     spark.stop()     
          
  }


   org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
   
  def createDS(saprk : SparkSession) : Dataset[Person] = {
       import saprk.implicits._
       

    val sparkHomeDir = "./data"
    saprk.read.textFile(sparkHomeDir + "/person.csv")
    .map(_.split(","))
    .map{ case Array(name, age, job) => Person(name, age.toInt, job) }
  }
  
   def createMissingDS(saprk : SparkSession) { //: Dataset[Person] = {
       import saprk.implicits._
     

    val sparkHomeDir = "./data"
   // val ds2 = saprk.read.option("header", "true").option("inferSchema",true).csv(sparkHomeDir + "/person.csv").as[Person]
    val ds2 = saprk.read
    .option("header", false)
                  .option("inferSchema",true)
                  .csv(sparkHomeDir + "/person.csv")
                  .toDF("name", "age", "Job")//
                  .as[Person]   
    //  val ds2 =saprk.read.option("header", "true").csv(sparkHomeDir + "/person.csv")
     ds2.printSchema() 
     ds2.show()
    //.option("delimiter",",")
    //.map{ case Array(name, age, job) => Person(name, age.toInt, job) }
  }
  
    
  def runSelectEx(spark : SparkSession, ds:Dataset[Person]) {
    import spark.implicits._
    ds.select(ds("name")).show()
    ds.select(ds("age").as[String]).show()
    ds.filter(_.Job == "student").map(col => (col.age, col.Job)).show()
    ds.groupByKey(_.age).count().show()
    ds.groupByKey(_.Job).agg(max("age").as[Long], countDistinct("age").as[Long]).show

  }
}