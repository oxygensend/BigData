// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------



// COMMAND ----------


import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)


val fileName = "dbfs:/FileStore/tables/pageviews_by_second.tsv"

val data = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)

data.write.mode("overwrite").parquet("dbfs:/pageviews_by_second.parquet")


// COMMAND ----------

 spark.catalog.clearCache()
val parquetDir = "dbfs:/pageviews_by_second.parquet"

val df = spark.read
  .parquet(parquetDir)
.repartition(2000)
//    .coalesce(6)
.groupBy("site").sum()


df.explain
df.count()

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

df.count()

// COMMAND ----------

df.repartition(1).count()

// COMMAND ----------

df.repartition(7).count()

// COMMAND ----------

df.repartition(9).count()

// COMMAND ----------

df.repartition(16).count()

// COMMAND ----------

df.repartition(200).count()

// COMMAND ----------

df.repartition(4000).count()

// COMMAND ----------

df.coalesce(1).count()

// COMMAND ----------

df.coalesce(2).count()

// COMMAND ----------

df.coalesce(3).count()

// COMMAND ----------

df.coalesce(4).count()

// COMMAND ----------

df.coalesce(5).count()

// COMMAND ----------

df.coalesce(6).count()

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val actorsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)


// COMMAND ----------

display(actorsDf)

// COMMAND ----------

import org.apache.spark.sql.Column
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

// Funkcja majaca na celu zliczenie wartosci null w danej kolumnie
def countNullColumn(colName : String, df :DataFrame) : Long ={
  if(!df.columns.contains(colName))
  {
      println("No such column in dataframe")
      return 1
  }
  


  
  return df.filter(col(colName).isNull).count()
}

// COMMAND ----------

countNullColumn("job", actorsDf)

// COMMAND ----------

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._

// Funkcja majaca na celu wyswietlenie wartosci srednich dla podanych kolumn z dataframe
def countMeansForColumns(columns: Array[String], df: DataFrame): Unit = {
  
  for(column <-columns){
    if(!df.columns.contains(column)){
  
      println("No such column in dataframe - " + column)
     
    } else {
      
      println(column + df.select(avg(col(column))).show())
      
  }
  }

}

// COMMAND ----------

countMeansForColumns(Array("ordering", "cdc"), actorsDf)

// COMMAND ----------

// Funkcja majaca na celu zamiane stringa na nowego
def replaceStrWithAnother(str_to_replace: String, new_statement: String, colName: String, df: DataFrame): DataFrame = {
   if(!df.columns.contains(colName)){
  
      println("No such column in dataframe")
      return spark.emptyDataFrame
    } 
    if(str_to_replace == ""){
      println("Cant replace empty string")
      return spark.emptyDataFrame

    }
    return df.withColumn(colName,regexp_replace(col(colName),lit(str_to_replace),lit(new_statement)))
    
  
}

// COMMAND ----------

val newDf = replaceStrWithAnother("actress", "actor", "category", actorsDf)
newDf

// COMMAND ----------


val filePath = "dbfs:/FileStore/tables/Files/actors.csv"
val actorsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

// Funkcja majaca na celu wczytanie pliku jako obiekt DataFrame
def readData(path: String, format: String, header: String, inferSchema: String ): DataFrame = {
  try {
  if(!Array("true", "false").contains(header)){
    throw new Exception("Heder must be 'true' or 'false")
    
  }
  if(!Array("true", "false").contains(inferSchema)){
    throw new Exception("inferSchema must be 'true' or 'false")
   
  }
  
  return spark.read.format(format)
          .option("header", header)
          .option("inferSchema", inferSchema)
          .load(path)
  }
  catch {
    case e: java.io.FileNotFoundException => {println("File doesnt exist"); return spark.emptyDataFrame}
    case e2: IllegalArgumentException => {println("Path must be absolute");  return spark.emptyDataFrame}
    case e3: Exception => {println(e3);  return spark.emptyDataFrame}
  }
  finally {
   
  }
}

// COMMAND ----------

val df = readData(filePath, "csv", "true", "true")
df.show()

// COMMAND ----------

//Funkcja majaca na celu zliczenia w danej kolumnie
def countOccurences(df: DataFrame, columns: Array[String]): Unit = {
  for(column <-columns){
    if(!df.columns.contains(column)){
  
      println("No such column in dataframe - " + column)
     
    } else {
      
      println(df.groupBy(column).count().show())
      
  }
  }
}

// COMMAND ----------

countOccurences(actorsDf, Array("category", "job"))

// COMMAND ----------

def deleteColumns(df: DataFrame, columns: Array[String]): DataFrame = {
  
  var temp_df = df
  for(column <-columns){
    if(!df.columns.contains(column)){
  
      println("No such column in dataframe - " + column)
     
    } else {
      
      temp_df = temp_df.drop(column)
     
  }
  }
  return temp_df
}

// COMMAND ----------

val df =deleteColumns(actorsDf, Array("category", "job"))
df.show()

// COMMAND ----------


