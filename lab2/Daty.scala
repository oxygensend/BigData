// Databricks notebook source
// MAGIC %md
// MAGIC Użyj każdą z tych funkcji 
// MAGIC * `unix_timestamp()` 
// MAGIC * `date_format()`
// MAGIC * `to_unix_timestamp()`
// MAGIC * `from_unixtime()`
// MAGIC * `to_date()` 
// MAGIC * `to_timestamp()` 
// MAGIC * `from_utc_timestamp()` 
// MAGIC * `to_utc_timestamp()`

// COMMAND ----------

import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "Date")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)

// COMMAND ----------


dataFrame.printSchema()

// COMMAND ----------

// MAGIC %md
// MAGIC ## unix_timestamp(..) & cast(..)

// COMMAND ----------

// MAGIC %md
// MAGIC Konwersja **string** to a **timestamp**.
// MAGIC 
// MAGIC Lokalizacja funkcji 
// MAGIC * `pyspark.sql.functions` in the case of Python
// MAGIC * `org.apache.spark.sql.functions` in the case of Scala & Java

// COMMAND ----------

// MAGIC %md
// MAGIC ## 1. Zmiana formatu wartości timestamp yyyy-MM-dd'T'HH:mm:ss 
// MAGIC `unix_timestamp(..)`
// MAGIC 
// MAGIC Dokumentacja API `unix_timestamp(..)`:
// MAGIC > Convert time string with given pattern (see <a href="http://docs.oracle.com/javase/tutorial/i18n/format/simpleDateFormat.html" target="_blank">SimpleDateFormat</a>) to Unix time stamp (in seconds), return null if fail.
// MAGIC 
// MAGIC `SimpleDataFormat` is part of the Java API and provides support for parsing and formatting date and time values.

// COMMAND ----------

val nowyunix = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss").cast("timestamp")).show()

// COMMAND ----------

// MAGIC %md
// MAGIC 2. Zmień format zgodnie z klasą `SimpleDateFormat`**yyyy-MM-dd HH:mm:ss**
// MAGIC   * a. Wyświetl schemat i dane żeby sprawdzicz czy wartości się zmieniły

// COMMAND ----------


val zmianaFormatu = dataFrame
  .withColumnRenamed("timestamp", "timestamp_formatted")
  .select( $"*", unix_timestamp($"timestamp_formatted", "yyyy-MM-dd HH:mm:ss") )

zmianaFormatu.printSchema()

zmianaFormatu.show()

// COMMAND ----------


val tempE = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxx", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .withColumnRenamed("CAST(unix_timestamp(capturedAt, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "xxxcast")

tempE.printSchema()
tempE.show()

// COMMAND ----------

// MAGIC %md
// MAGIC ## Stwórz nowe kolumny do DataFrame z wartościami year(..), month(..), dayofyear(..)

// COMMAND ----------

val newDf= dataFrame
  .withColumn("year", year($"timestamp"))
  .withColumn("month", month($"timestamp"))
  .withColumn("dayofyear", dayofyear($"timestamp"))
 
                   
display(newDf)

// COMMAND ----------

val zmianaFormatu = dataFrame
  .select( $"*", date_format($"current_date", "yyyy/MM "))

zmianaFormatu.printSchema()

display(zmianaFormatu)

// COMMAND ----------

val zmianaFormatu = dataFrame
  .select( $"timestamp", from_unixtime($"unix"))

zmianaFormatu.printSchema()

display(zmianaFormatu)

// COMMAND ----------

val zmianaFormatu = dataFrame
  .select( $"Date", to_date($"Date", "MMM, yyyy"))

zmianaFormatu.printSchema()

display(zmianaFormatu)

// COMMAND ----------

val zmianaFormatu = dataFrame
  .select( $"timestamp", to_timestamp($"timestamp", "yyyy-MM-dd'T'HH:mm:ss").as("timestamp1"))

zmianaFormatu.printSchema()

display(zmianaFormatu)

// COMMAND ----------

val kolumny = Seq("ts","tz")
val dane = Seq(("1997-02-28 10:30:00", "JST"))


var df = spark.createDataFrame(dane).toDF(kolumny:_*)

df.printSchema()
display(df)

// COMMAND ----------

val zmianaFormatu = dataFrame
  .select( $"timestamp", from_utc_timestamp($"timestamp", "Asia/Seoul").as("Seoul"))


zmianaFormatu.printSchema()

display(zmianaFormatu)

// COMMAND ----------

val zmianaFormatu = dataFrame
  .select( $"timestamp", to_utc_timestamp($"timestamp", "Asia/Seoul").as("Seoul"))


zmianaFormatu.printSchema()

display(zmianaFormatu)
