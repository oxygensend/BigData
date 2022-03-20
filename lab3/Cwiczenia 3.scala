// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

import org.apache.spark.sql.functions._
val startTime = System.currentTimeMillis()
// your code goes here



val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)


val endTime = System.currentTimeMillis()
val elapsedSeconds = (endTime - startTime)

val newDf = namesDf
            .withColumn("time", lit(elapsedSeconds))
            .withColumn("height_ft", $"height"/30.48)
display(newDf)

// COMMAND ----------

val name = namesDf.select(split(col("name")," ").getItem(0).as("FirstName"),
    split(col("name")," ").getItem(1).as("Surname"))
    .drop("name")
    .groupBy($"FirstName")
    .count()
    .orderBy(desc("count"))
    .take(1)


display(name)

// COMMAND ----------

val df = namesDf.withColumn("age", months_between(current_date(),$"date_of_birth")/12)

display(df)

// COMMAND ----------

val df_dropped=newDf.drop($"bio").drop($"death_details")

display(df_dropped)

// COMMAND ----------

 var new_col = newDf
  for(col <- newDf.columns){
    new_col = new_col.withColumnRenamed(col,col.replaceAll("_", " "))
  }
 new_col = new_col.toDF(new_col.columns map(_.toUpperCase): _*)
display(new_col)

// COMMAND ----------

var df_sortted = new_col.orderBy("name")

display(df_sortted)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

val startTime = System.currentTimeMillis()
val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)


val newDf = namesDf.withColumn("movie_age", year(current_date()) - $"year")
            .withColumn("budget",regexp_replace($"budget", "[^0-9]",""))
            .na.drop()

val endTime = System.currentTimeMillis()
val elapsedSeconds = (endTime - startTime)

val df = newDf.withColumn("time", round(lit(elapsedSeconds)/1000,2))
df.explain


// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val startTime = System.currentTimeMillis()
val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

// podkunkt 3 juz jest, wiec nie liczylem

// val columns_to_sum = List(col("votes_1"),col("votes_2"), col("votes_3"), col("votes_4"), col("votes_5"), col("votes_6"), col("votes_7"), col("votes_8"), col("votes_9"), col("votes_10"))
val df = namesDf.na.drop()
         .withColumn("avg-mean",  $"mean_vote" - $"weighted_average_vote" )
         .withColumn("avg-median",   $"median_vote" - $"weighted_average_vote" )


         
val compare = df.select(avg($"females_allages_avg_vote"), avg($"males_allages_avg_vote"))

val endTime = System.currentTimeMillis()
val elapsedSeconds = (endTime - startTime)
val new_df = df.withColumn("time", round(lit(elapsedSeconds)/1000,2))
display(compare)


// COMMAND ----------

display(new_df)

// COMMAND ----------

// MAGIC %md
// MAGIC # Zadanie 2
// MAGIC 
// MAGIC  - Jobs - W kazdym elemencie spark UI mozna znalezc prace wykonaną przez dany klaster wraz iloscią wykonanych taskow w danym jobie,  dacie wykonania oraz czasie trwania 
// MAGIC  - Stages - są to kolejne kroki wykonania planu, zbiór taskow dla danego zadania?
// MAGIC  - Storage - przedstawia ilosc danych odczytanych, wpisanych itp z systemu plików
// MAGIC  - enviroment - ukazuje jakie srodowisko jest przez nas uzywane 
// MAGIC  - execturos - pamiec jaka zostala uzyta w danym klastrze?
// MAGIC  - sql - pokazuje uzyte polecania sql
// MAGIC  - jdbc/odbc - pokazuje statystyki klastra
// MAGIC  
// MAGIC  

// COMMAND ----------

// MAGIC %md
// MAGIC # Zadanie 3

// COMMAND ----------

val test = df.groupBy("year").count().orderBy(desc("count")).explain



// COMMAND ----------

// MAGIC %md
// MAGIC # Zadanie 4

// COMMAND ----------

var username = "sqladmin"
var password = "$3bFHs56&o123$" 
val dataFromSqlServer = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable","BuildVersion")
      .option("user", username)
      .option("password",password)
      .load()

display(dataFromSqlServer)
