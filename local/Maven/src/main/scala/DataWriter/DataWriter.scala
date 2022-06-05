package DataWriter

import org.apache.spark.sql.SparkSession

object DataWriter {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Moja-aplikacja")
      .getOrCreate()


    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("movies.csv")


  }
}