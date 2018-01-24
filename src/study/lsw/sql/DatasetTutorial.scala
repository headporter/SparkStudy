package study.lsw.sql

/*
 * https://www.balabit.com/blog/spark-scala-dataset-tutorial/
 */

import org.apache.spark.sql.{ Dataset, SparkSession, DataFrame }
import org.apache.spark.sql.functions._
import org.apache.spark.sql._

object DatasetTutorial {

  case class Friends(name: String, friends: String)
  case class Friends_Missing(Who: String, friends: Option[String])

  def main(args: Array[String]) {
    val spark = SparkSession
      .builder()
      .appName("DS test")
      .master("local[*]")
      .config("spark.driver.host", "127.0.0.1")
      .getOrCreate()

    import spark.implicits._

    val df = Seq(
      ("Yoda", "Obi-Wan Kenobi"),
      ("Anakin Skywalker", "Sheev Palpatine"),
      ("Luke Skywalker", "Han Solo, Leia Skywalker"),
      ("Leia Skywalker", "Obi-Wan Kenobi"),
      ("Sheev Palpatine", "Anakin Skywalker"),
      ("Han Solo", "Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi, Chewbacca"),
      ("Obi-Wan Kenobi", "Yoda, Qui-Gon Jinn"),
      ("R2-D2", "C-3PO"),
      ("C-3PO", "R2-D2"),
      ("Darth Maul", "Sheev Palpatine"),
      ("Chewbacca", "Han Solo"),
      ("Lando Calrissian", "Han Solo"),
      ("Jabba", "Boba Fett"))
      .toDF("name", "friends")

    df.show()

    val friends_ds = df.as[Friends]

    friends_ds.show()

    val ds_missing = Seq(
      ("Yoda", Some("Obi-Wan Kenobi")),
      ("Anakin Skywalker", Some("Sheev Palpatine")),
      ("Luke Skywalker", None),  // null
      ("Leia Skywalker", Some("Obi-Wan Kenobi")),
      ("Sheev Palpatine", Some("Anakin Skywalker")),
      ("Han Solo", Some("Leia Skywalker, Luke Skywalker, Obi-Wan Kenobi")))
      .toDF("Who", "friends")
      .as[Friends_Missing]
    
    ds_missing.show()
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
    //characters_BadType_ds2.filter(x=> x.weight>79).show()  //error
    //characters_BadType_ds2.filter(x=> x.weight!=null && x.weight>79).show()
    
    //characters_ds.filter(x=> x.weight > 79).show()  //
    //characters_BadType_ds.filter(x=> x.weight>79).show()
    /*
    
   //org.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)
   // characters_ds.filter(x=> x.weight != null && x.weight > 79).show()
    *
    */
  }

}