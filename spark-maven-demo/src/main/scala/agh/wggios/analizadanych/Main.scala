package agh.wggios.analizadanych
import org.apache.spark.sql.{ SparkSession}
import org.apache.spark.sql.DataFrame
import datareader.DataReader
import datawriter.DataWriter
import org.apache.log4j.Logger
object Main {
  @transient lazy val logger = Logger.getLogger(getClass.getName)
  def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder
        .master("local[4]")
        .appName("Maven_app")
        .getOrCreate();

    import spark.implicits._
    val reader= new DataReader();
    logger.info("*********Reading data****************");
    val df:DataFrame=reader.read_csv("movies.csv", spark.sqlContext, header = true );
    df.show(10);

    val new_df = df.drop("title");
    val writer=new DataWriter();
    writer.write(new_df,"cleared_movies.csv");

  }
}
