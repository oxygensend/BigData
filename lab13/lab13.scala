// Databricks notebook source
import org.apache.spark.sql.functions._

val actorsPath = "dbfs:/FileStore/tables/Files/actors.csv"
val dfActors = spark.read.option("header","true").option("inferSchema","true").format("csv").load(actorsPath)


// COMMAND ----------

display(dfActors)

// COMMAND ----------

dfActors.write.format("parquet").mode("overwrite").partitionBy("category").saveAsTable("AfterPartitioning")

// COMMAND ----------

dfActors.write.format("parquet").mode("overwrite").bucketBy(10,"category").saveAsTable("AfterBucketing")

// COMMAND ----------

// MAGIC %sql
// MAGIC ANALYZE TABLE AfterPartitioning COMPUTE STATISTICS FOR ALL COLUMNS;

// COMMAND ----------

// MAGIC %sql
// MAGIC DESC EXTENDED AfterPartitioning;

// COMMAND ----------

// MAGIC %sql
// MAGIC ANALYZE TABLE AfterBucketing COMPUTE STATISTICS FOR ALL COLUMNS;

// COMMAND ----------

// MAGIC %sql
// MAGIC DESC EXTENDED AfterBucketing;
