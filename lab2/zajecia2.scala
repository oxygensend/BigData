// Databricks notebook source
// MAGIC %md
// MAGIC ## Zadanie 1
// MAGIC 
// MAGIC Wybierz jeden z plików csv z poprzednich ćwiczeń i stwórz ręcznie schemat danych. Stwórz DataFrame wczytując plik z użyciem schematu. 

// COMMAND ----------

// MAGIC %scala
// MAGIC display(dbutils.fs.ls("dbfs:/FileStore/tables/Files/movies.csv"))

// COMMAND ----------


val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
            .option("header","true")
            .option("inferSchema","true")
            .load(filePath)
display(moviesDf)


// COMMAND ----------

import org.apache.spark.sql.types.{IntegerType, StringType, StructType, DoubleType, DateType,  StructField}



val schemat = StructType(Array(
  StructField("imdb_title_id", StringType, false),
  StructField("title", StringType, false),
  StructField("original_title", StringType, false),
  StructField("year", StringType, false),
  StructField("date_published", StringType, false),
  StructField("genre", StringType, false),
  StructField("duration", IntegerType, false),
  StructField("country", StringType, false),
  StructField("language", StringType, false),
  StructField("director", StringType, false),
  StructField("writer", StringType, false),
  StructField("production_company", StringType, false),
  StructField("actors", StringType, false),
  StructField("description", StringType, false),
  StructField("avg_vote", StringType, false),
  StructField("votes", IntegerType, false),
  StructField("budget", StringType, false),
  StructField("usa_gross_income", StringType, false),
  StructField("worlwide_gross_income", StringType, false),
  StructField("metascore", DoubleType, false),
  StructField("reviews_from_users", DoubleType, false),
  StructField("reviews_from_critics", DoubleType, false)
  
))

val file = spark.read.format("csv")
.option("header", "true")
.schema(schemat)
.load("dbfs:/FileStore/tables/Files/movies.csv")


// COMMAND ----------


display(file)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 2
// MAGIC 
// MAGIC Użyj kilku rzędów danych z jednego z plików csv i stwórz plik json. Stwórz schemat danych do tego pliku. Przydatny tool to sprawdzenie formatu danych. https://jsonformatter.curiousconcept.com/ 

// COMMAND ----------

import java.io.File
import java.nio.file.{Paths, Files}
import java.nio.charset.StandardCharsets
import org.apache.spark.sql.types._

val filePath = "dbfs:/FileStore/tables/actors.json"

val schema=new StructType()
.add("imdb_title_id",StringType, true)
.add("ordering", IntegerType, true)
.add("imdb_name",StringType, true)
.add("category",StringType, true)
.add("job", StringType, true)
.add("characters", StringType, true)

val df_json = spark.read.format("json").option("multiLine", true).schema(schema_json).load(filePath)
display(df_json)

// COMMAND ----------

// MAGIC %sh ls ../*

// COMMAND ----------

display(dbutils.fs.ls("dbfs:/FileStore/tables/movies.json"))

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 4
// MAGIC 
// MAGIC Wykorzystaj posiadane pliki bądź dodaj nowe i użyj wszystkich typów oraz ‘badRecordsPath’, zapisz co się dzieje. Jeśli jedna z opcji nie da żadnych efektów, trzeba popsuć dane. 

// COMMAND ----------

val file = spark.read.format("csv")
.option("header", "true")
.schema(schemat)
.option("badRecordsPath", "/mnt/source/badrecords")
.load("dbfs:/FileStore/tables/Files/movies.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC PERMISSIVE: Jeśli atrybuty nie mogą zostać wczytane Spark zamienia je na nule​
// MAGIC 
// MAGIC DROPMALFORMED: wiersze są usuwane​
// MAGIC 
// MAGIC FAILFAST: proces odczytu zostaje całkowicie zatrzymany​

// COMMAND ----------

val file = spark.read.format("csv")
.option("header", "true")
.schema(schemat)
.option("mode", "PERMISSIVE")
.load("dbfs:/FileStore/tables/Files/movies.csv")

// COMMAND ----------

val file = spark.read.format("csv")
.option("header", "true")
.schema(schemat)
.option("mode", "DROPMALFORMED")
.load("dbfs:/FileStore/tables/Files/movies.csv")

// COMMAND ----------

val file = spark.read.format("csv")
.option("header", "true")
.schema(schemat)
.option("mode", "FAILFAST")
.load("dbfs:/FileStore/tables/Files/movies.csv")

// COMMAND ----------

// MAGIC %md
// MAGIC ## Zadanie 5
// MAGIC 
// MAGIC Zapisz jeden z wybranych plików do formatów (‘.parquet’, ‘.json’). Sprawdź, czy dane są zapisane poprawnie, użyj do tego DataFrameReader. Opisz co widzisz w docelowej ścieżce i otwórz używając DataFramereader. 
// MAGIC 
// MAGIC  

// COMMAND ----------

moviesDf.write.parquet("dbfs:/FileStore/tables/Files/movies.parquet")

// COMMAND ----------

val paraquetDf = spark.read.parquet("dbfs:/FileStore/tables/Files/movies.parquet")
display(paraquetDf)
