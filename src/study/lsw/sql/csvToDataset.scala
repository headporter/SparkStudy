package study.lsw.sql

/*
 * https://blog.codecentric.de/en/2016/07/spark-2-0-datasets-case-classes/
 */

import org.apache.spark.sql.{ Dataset, SparkSession, DataFrame }
import org.apache.spark.SparkContext
import org.apache.spark.sql.{ DataFrame, Row }
import org.apache.spark.sql.functions._

object csvToDataset {
  case class Body(
    id:       Int,
    width:    Double,
    height:   Double,
    depth:    Double,
    material: String,
    color:    String)

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("csvToDataframe")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    val df: DataFrame = spark.read
      .option("header", true)
      .option("inferSchema", "true")
      .csv("./data/bodies.csv")

    //df.show()
    df.printSchema()
    import spark.implicits._
    val ds = df.as[Body]
    ds.printSchema()

    import org.apache.spark.sql.expressions.scalalang.typed.{
      count => typedCount,
      sum => typedSum
    }

    ds.groupByKey(body => body.color)
      .agg(
        typedCount[Body](_.id).name("count(id)"),
        typedSum[Body](_.width).name("sum(width)"),
        typedSum[Body](_.height).name("sum(height)"),
        typedSum[Body](_.depth).name("sum(depth)"))
      .withColumnRenamed("value", "group")
      .alias("Summary by color level")
      .show()

    val volumeUDF = udf {
      (width: Double, height: Double, depth: Double) => width * height * depth
    }

    ds.withColumn("volume", volumeUDF($"width", $"height", $"depth")).show()

  }

}