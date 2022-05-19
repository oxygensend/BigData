
import org.apache.spark.sql.SparkSession


object main {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .master("local[4]")
      .appName("Moja-aplikacja")
      .getOrCreate()


    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("movies.csv")

    val newDf = df.drop(df("original_title"));
    newDf.show()

  }
}
