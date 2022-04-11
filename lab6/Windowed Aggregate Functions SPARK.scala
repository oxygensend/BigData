// Databricks notebook source
// MAGIC %sql
// MAGIC Create database if not exists Sample

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Transactions ( AccountId INT, TranDate DATE, TranAmt DECIMAL(8, 2));
// MAGIC 
// MAGIC CREATE TABLE IF NOT EXISTS Sample.Logical (RowID INT,FName VARCHAR(20), Salary SMALLINT, TransactionID INT );

// COMMAND ----------

// MAGIC %sql
// MAGIC 
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
// MAGIC ( 3, '2011-01-30', 2500)

// COMMAND ----------

// MAGIC %sql
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

// MAGIC %md 
// MAGIC Totals based on previous row

// COMMAND ----------

// MAGIC %md
// MAGIC # Zadanie 1

// COMMAND ----------

import org.apache.spark.sql.functions._

val transactions = spark.read.table("Sample.Transactions")
val logical = spark.read.table("Sample.Logical")

// COMMAND ----------

// MAGIC %sql
// MAGIC 
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTotalAmt
// MAGIC FROM Sample.Transactions ORDER BY AccountId, TranDate;

// COMMAND ----------

import org.apache.spark.sql.expressions.Window
display(
        transactions.select($"AccountId",
                            $"TranDate",
                            $"TranAmt",
                            sum("TranAmt").over(Window.partitionBy("AccountId").orderBy("TranDate")) as "RunTotalAmt"
                           )
)

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- running average of all transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunAvg,
// MAGIC -- running total # of transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTranQty,
// MAGIC -- smallest of the transactions so far
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunSmallAmt,
// MAGIC -- largest of the transactions so far
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunLargeAmt,
// MAGIC -- running total of all transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate) as RunTotalAmt
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId,TranDate;

// COMMAND ----------

val windowSpec = Window.partitionBy("AccountId").orderBy("TranDate")
display(
        transactions.select($"AccountId",
                            $"TranDate",
                            $"TranAmt",
                            avg("TranAmt").over(windowSpec) as "RunAvg",
                            count("AccountId").over(windowSpec) as "RunTranQty",
                            min("TranAmt").over(windowSpec) as "RunSmallAmt",
                            max("TranAmt").over(windowSpec) as "RunLargeAmt",
                            sum("TranAmt").over(windowSpec) as "RunTotalAmt"
                           ).orderBy("AccountId","TranDate")
)

// COMMAND ----------

// MAGIC %md 
// MAGIC * Calculating Totals Based Upon a Subset of Rows

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT AccountId,
// MAGIC TranDate,
// MAGIC TranAmt,
// MAGIC -- average of the current and previous 2 transactions
// MAGIC AVG(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideAvg,
// MAGIC -- total # of the current and previous 2 transactions
// MAGIC COUNT(*) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideQty,
// MAGIC -- smallest of the current and previous 2 transactions
// MAGIC MIN(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMin,
// MAGIC -- largest of the current and previous 2 transactions
// MAGIC MAX(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideMax,
// MAGIC -- total of the current and previous 2 transactions
// MAGIC SUM(TranAmt) OVER (PARTITION BY AccountId ORDER BY TranDate ROWS BETWEEN 2 PRECEDING AND CURRENT ROW) as SlideTotal,
// MAGIC ROW_NUMBER() OVER (PARTITION BY AccountId ORDER BY TranDate) AS RN
// MAGIC FROM Sample.Transactions 
// MAGIC ORDER BY AccountId, TranDate, RN

// COMMAND ----------

val windowSpec = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween( -2, Window.currentRow)
display(
        transactions.select($"AccountId",
                            $"TranDate",
                            $"TranAmt",
                            avg("TranAmt").over(windowSpec) as "SlideAvg",
                            count("AccountId").over(windowSpec) as "SlideQty",
                            min("TranAmt").over(windowSpec) as "SlideMin",
                            max("TranAmt").over(windowSpec) as "SlideMax",
                            sum("TranAmt").over(windowSpec) as "SlideTotal",
                            row_number().over(Window.partitionBy("AccountId").orderBy("TranDate")) as "RN"
                           ).orderBy("AccountId","TranDate")
)

// COMMAND ----------

// MAGIC %md
// MAGIC * Logical Window

// COMMAND ----------

// MAGIC %sql
// MAGIC SELECT RowID,
// MAGIC FName,
// MAGIC Salary,
// MAGIC SUM(Salary) OVER (ORDER BY Salary ROWS UNBOUNDED PRECEDING) as SumByRows,
// MAGIC SUM(Salary) OVER (ORDER BY Salary RANGE UNBOUNDED PRECEDING) as SumByRange
// MAGIC 
// MAGIC FROM Sample.Logical
// MAGIC ORDER BY RowID;

// COMMAND ----------

display(
        logical.select(
                       $"RowId",
                       $"FName",
                       $"Salary",
                       sum("salary").over(Window.orderBy("Salary")
                                                .rowsBetween(Window.unboundedPreceding,Window.currentRow)
                                         ) as "SumByRows",
                       sum("salary").over(Window.orderBy("salary")
                                                .rangeBetween(Window.unboundedPreceding, Window.currentRow)
                                         ) as "SumByRange"
        
        )
)

// COMMAND ----------

// MAGIC %md
// MAGIC # Zadanie 2

// COMMAND ----------

val between = Window.orderBy("TranDate").rowsBetween( -2, Window.currentRow)
val rows = Window.orderBy("TranDate").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val range = Window.orderBy("TranDate").rangeBetween(Window.unboundedPreceding, Window.currentRow)

display(
        transactions.select($"AccountId",
                            $"TranDate",
                            $"TranAmt",
                     
                            first("TranAmt").over(between) as  "first_value_between",
                            first("TranAmt").over(rows) as  "first_value_rows",
                            first("TranAmt").over(range) as "first_value_range",
                            last("TranAmt").over(between) as  "last_value_between",
                            last("TranAmt").over(rows) as  "last_value_rows",
                            last("TranAmt").over(range) as "last_value_range",
                            row_number().over(Window.partitionBy("AccountId").orderBy("TranDate")) as "rn",
                            dense_rank().over(Window.partitionBy("AccountId").orderBy("TranDate")) as "dens_rank",
                            
                            
                           ).orderBy("AccountId","TranDate")
)

// COMMAND ----------

// MAGIC %md
// MAGIC 
// MAGIC # Zadanie 3

// COMMAND ----------

val joinExpression = transactions.col("AccountId") === logical.col("TransactionID")




// COMMAND ----------

val left_semi = logical.join(transactions, joinExpression, "left_semi")

left_semi.explain


// COMMAND ----------

left_semi.show()

// COMMAND ----------

val left_anti = logical.join(transactions, joinExpression, "left_anti")

left_anti.explain

// COMMAND ----------

left_anti.show()

// COMMAND ----------

# Zadanie 4

// COMMAND ----------

val person = Seq(
    (0, "Bill Chambers", 0, Seq(100)),
    (1, "Matei Zaharia", 1, Seq(500, 250, 100)),
    (2, "Michael Armbrust", 1, Seq(250, 100)))
  .toDF("id", "name", "graduate_program", "spark_status")
val graduateProgram = Seq(
    (0, "Masters", "School of Information", "UC Berkeley"),
    (2, "Masters", "EECS", "UC Berkeley"),
    (1, "Ph.D.", "EECS", "UC Berkeley"))
  .toDF("id", "degree", "department", "school")
val sparkStatus = Seq(
    (500, "Vice President"),
    (250, "PMC Member"),
    (100, "Contributor"))
  .toDF("id", "status")


// COMMAND ----------

val joinExpression = person.col("graduate_program") === graduateProgram.col("id")
person.join(graduateProgram, joinExpression).show()

// COMMAND ----------

// MAGIC %md
// MAGIC # Zadanie 5

// COMMAND ----------

val broadcast_join = person.join(broadcast(graduateProgram), joinExpression)
broadcast_join.explain

// COMMAND ----------

broadcast_join.show()
