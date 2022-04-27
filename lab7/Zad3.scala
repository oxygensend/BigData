// Databricks notebook source
// MAGIC %sql
// MAGIC Create database if not exists Sample

// COMMAND ----------

// MAGIC %sql
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate DATE, TranAmt DECIMAL(8, 2));
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT, TransactionID INT );

// COMMAND ----------

// MAGIC %sql
// MAGIC INSERT INTO Sample.Transactions VALUES 
// MAGIC ( 1, '2011-01-01', 500),
// MAGIC ( 1, '2011-01-15', 50),
// MAGIC ( 1, '2011-01-22', 250),
// MAGIC ( 1, '2011-01-24', 75),
// MAGIC ( 1, '2011-01-26', 125),
// MAGIC ( 1, '2011-01-28', 175),
// MAGIC ( 2, '2011-01-01', 500),
// MAGIC ( 2, '2011-01-15', 50),
// MAGIC ( 2, '2011-01-22', 25),
// MAGIC ( 2, '2011-01-23', 125),
// MAGIC ( 2, '2011-01-26', 200),
// MAGIC ( 2, '2011-01-29', 250),
// MAGIC ( 3, '2011-01-01', 500),
// MAGIC ( 3, '2011-01-15', 50 ),
// MAGIC ( 3, '2011-01-22', 5000),
// MAGIC ( 3, '2011-01-25', 550),
// MAGIC ( 3, '2011-01-27', 95 ),
// MAGIC ( 3, '2011-01-30', 2500);
// MAGIC 
// MAGIC 
// MAGIC INSERT INTO Sample.Logical
// MAGIC VALUES (1,'George', 800, 1),
// MAGIC (2,'Sam', 950, 1),
// MAGIC (3,'Diane', 1100, 1),
// MAGIC (4,'Nicholas', 1250,2),
// MAGIC (5,'Samuel', 1250,2 ),
// MAGIC (6,'Patricia', 1300,1),
// MAGIC (7,'Brian', 1500,3),
// MAGIC (8,'Thomas', 1600,3),
// MAGIC (9,'Fran', 2450,2),
// MAGIC (10,'Debbie', 2850,3),
// MAGIC (11,'Mark', 2975,3),
// MAGIC (12,'James', 3000,1),
// MAGIC (13,'Cynthia', 3000,1),
// MAGIC (14,'Christopher', 5000,3);

// COMMAND ----------

import org.apache.spark.sql.functions._

val transactions = spark.read.table("Sample.Transactions")

display(transactions)

// COMMAND ----------



def clearDatabase( database: String ) = {
  val tables = spark.catalog.listTables(database)
  val names_list = tables.select("name").map(r => r.getString(0)).collect.toList 
  names_list.foreach(name => sqlContext.sql("DELETE  FROM " + database + "." + name))
}





// COMMAND ----------

clearDatabase("Sample")

val transactions = spark.read.table("Sample.Transactions")

display(transactions)
