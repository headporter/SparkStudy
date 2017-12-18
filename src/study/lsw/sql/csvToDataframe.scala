package study.lsw.sql

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, Row }

object csvToDataframe {
  case class Bank(age: Integer, job: String, marital: String, education: String, balance: Integer)
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("csvToDataframe")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    val sc = spark.sparkContext
    val bankText = sc.textFile("./data/bank-full.csv")
    //val bankText = sc.textFile("/Users/1002516/20_Dev/zeppelin-0.7.3-bin-all/data/bank/bank-full.csv")
    println(bankText.count())
    //bankText.map(s => s.split(";"))
    
    val bank = bankText.map(s => s.split(";")).filter(s => s(1) != "\"job\"").map(
      s => Bank(
        s(0).toInt,
        s(1).replaceAll("\"", ""),
        s(2).replaceAll("\"", ""),
        s(3).replaceAll("\"", ""),
        s(5).replaceAll("\"", "").toInt))

    import spark.implicits._
    // convert to DataFrame and create temporal table

    val tbl = bank.toDF()
    tbl.show()
    tbl.printSchema()

  }

}