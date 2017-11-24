package study.lsw.sql

import org.apache.spark.sql.{Dataset, SparkSession}

object DataSet {
  def main (args : Array[String]) {
    val spark = SparkSession
          .builder()
          .appName("DS test")
          .master("local[*]")
          .config("spark.driver.host","127.0.0.1")
          .getOrCreate()
          
     val ds = createDS(spark)     
     ds.show()   
          
     spark.stop()     
          
  }
  //: Dataset[String] = 
         case class Person (name : String, age : Long, Job : String)   
  
  def createDS(saprk : SparkSession) : Dataset[Person] = {
       import saprk.implicits._
       

    val sparkHomeDir = "./data"
    saprk.read.textFile(sparkHomeDir + "/person.csv")
    .map(_.split(","))
    .map{ case Array(name, age, job) => Person(name, age.toInt, job) }
    //.as[Person]
  }
}