package study.lsw.sql

import org.apache.spark.sql.{ Dataset, SparkSession, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object DSTest {
  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("DS test")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    import spark.implicits._

    val ds_missing = Seq(
      ("Yoda", Some("Obi-Wan Kenobi"), 1),
      ("Anakin Skywalker", Some("Sheev Palpatine"), 2),
      ("Luke Skywalker", None, 1),
      ("Leia Skywalker", Some("Obi-Wan Kenobi"), 1),
      ("Sheev Palpatine", Some("Anakin Skywalker"), 4),
      ("Han Solo", Some("Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi"), 1))
      .toDF("name", "friends", "no")
      .as[Friends]
    //.as[Friends_Missing]

    ds_missing.show
    //ds_missing.filter(_.friends!="Luke Skywalker").show

    //friends_ds.show()
    val characters_ds: Dataset[Characters] = spark
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("./data/StarWars.csv")
      .as[Characters]

    characters_ds.show()

    val characters_BadType_ds: Dataset[Characters_BadType] = spark
      .read
      .option("header", "true")
      .option("delimiter", ";")
      .option("inferSchema", "true")
      .csv("./data/StarWars.csv")
      .as[Characters_BadType]
    
    characters_BadType_ds.show()
    val characters_BadType_ds2 = characters_BadType_ds.filter(x=> x.jedi=="no_jedi")
    characters_BadType_ds2.show()
    //characters_BadType_ds2.filter(x=> x.weight>79).show()
    //characters_BadType_ds.filter(x=> x.weight>79).show()
    //characters_BadType_ds2.filter(x=> x.weight!=null && x.weight>79).show()
   //org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this) 
   // characters_ds.filter(x=> x.weight != null && x.weight > 79).show()
  }

}