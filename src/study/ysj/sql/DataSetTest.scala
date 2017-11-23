package study.ysj.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.ml

case class Person(name: String, age: Int, job: String)

object DataSetTest {
  def main(args:Array[String]) = {
    val spark = SparkSession
      .builder()
      .appName("basic")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()
    
    import spark.implicits._
    val srcDir = "./data"
    val ds = spark
      .read
      .textFile(srcDir + java.io.File.separatorChar + "person.csv")
      .map(_.split(","))
      .map{ case Array(name, age, job) => Person(name, age.toInt, job) }
      .persist()
      
    ds.groupByKey(_.job).count().show(false)
    ds.groupByKey(_.job).agg(max("age").as[Int], countDistinct("age").as[Long]).show()
    ds.groupByKey(_.job).mapValues(p => p.name + "(" + p.age + ")").reduceGroups((s1, s2) => s1 + s2).show()
  }
}