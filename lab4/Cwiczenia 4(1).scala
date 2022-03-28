// Databricks notebook source
// MAGIC %md 
// MAGIC Wykożystaj dane z bazy 'bidevtestserver.database.windows.net'
// MAGIC ||
// MAGIC |--|
// MAGIC |SalesLT.Customer|
// MAGIC |SalesLT.ProductModel|
// MAGIC |SalesLT.vProductModelCatalogDescription|
// MAGIC |SalesLT.ProductDescription|
// MAGIC |SalesLT.Product|
// MAGIC |SalesLT.ProductModelProductDescription|
// MAGIC |SalesLT.vProductAndDescription|
// MAGIC |SalesLT.ProductCategory|
// MAGIC |SalesLT.vGetAllCategories|
// MAGIC |SalesLT.Address|
// MAGIC |SalesLT.CustomerAddress|
// MAGIC |SalesLT.SalesOrderDetail|
// MAGIC |SalesLT.SalesOrderHeader|

// COMMAND ----------

// MAGIC %md
// MAGIC # Zadanie 1

// COMMAND ----------

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val table = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(table)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------


val jb = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")

val Customer = jb.option("dbtable", "SalesLT.Customer").load()
val ProductModel = jb.option("dbtable", "SalesLT.ProductModel").load()
val vProductModelCatalogDescription = jb.option("dbtable", "SalesLT.vProductModelCatalogDescription").load()
val ProductDescription = jb.option("dbtable", "SalesLT.ProductDescription").load()
val Product = jb.option("dbtable", "SalesLT.Product").load()
val ProductModelProductDescription = jb.option("dbtable", "SalesLT.ProductModelProductDescription").load()
val ProductCategory = jb.option("dbtable", "SalesLT.ProductCategory").load()
val vGetAllCategories = jb.option("dbtable", "SalesLT.vGetAllCategories").load()
val Address = jb.option("dbtable", "SalesLT.Address").load()
val CustomerAddress = jb.option("dbtable", "SalesLT.CustomerAddress").load()
val SalesOrderDetail = jb.option("dbtable", "SalesLT.SalesOrderDetail").load()
val SalesOrderHeader = jb.option("dbtable", "SalesLT.SalesOrderHeader").load()

val all_df = Array(Customer, ProductModel, vProductModelCatalogDescription, ProductDescription, Product, ProductModelProductDescription, ProductCategory, vGetAllCategories, Address, CustomerAddress, SalesOrderDetail, SalesOrderHeader)

// COMMAND ----------

// MAGIC %md
// MAGIC  Uzycie Nulls, fill, drop, replace, i agg
// MAGIC  * W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach
// MAGIC  * Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null
// MAGIC  * Użyj funkcji drop żeby usunąć nulle, 
// MAGIC  * wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]
// MAGIC  * Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg() 
// MAGIC    - Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

// COMMAND ----------

import org.apache.spark.sql.functions._

def countNulls(columns:Array[String])={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}


all_df.map( df => df.select(countNulls(df.columns):_*).show())





// COMMAND ----------

val filled_na = all_df.map( df => df.na.fill("tutaj byl null"))

display(filled_na.head)

// COMMAND ----------

val dropped_na = all_df.map( df => df.na.drop())

display(dropped_na.head)

// COMMAND ----------

//avg
val agg = SalesOrderHeader.select(count("TaxAmt") , count("Freight"),
                                   mean("TaxAmt"), mean("Freight"), first("TaxAmt"), first("Freight"))

display(agg)

// COMMAND ----------

// Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
// Użyj conajmniej dwóch overloded funkcji agregujących np z (Map)

val Product_agg = Product.groupBy("ProductModelId", "Color", "ProductCategoryID")
                         .agg("Color" -> "count", "StandardCost"->"sum", "weight"->"max")
                         
    

display(Product_agg)

// COMMAND ----------

// MAGIC %md
// MAGIC # Zadanie 2

// COMMAND ----------

import org.apache.spark.sql.functions.udf

val subRegion = udf((x: String) => x.takeWhile(_ != '-'))
val pow = udf((x: Double) => x*x)
val sqrt =  udf((x: Double) => scala.math.sqrt(x))



Product.select( $"StandardCost", pow($"StandardCost").as("StandardCostPow"),
                $"ListPrice", sqrt($"ListPrice").as("ListPriceSquare"),
                $"ProductNumber", subRegion($"ProductNumber").as("ProductRegion")).show()


// COMMAND ----------

display(Product)

// COMMAND ----------

// MAGIC %md
// MAGIC # Zadanie 3
// MAGIC ## Flatten json

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,ArrayType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode_outer


val path = "dbfs:/FileStore/tables/brzydki.json"
val jsonDF = spark.read.option("multiline","true").json(path)



  def flattenDataframe(df: DataFrame): DataFrame = {

    val fields = df.schema.fields
    val fieldNames = fields.map(x => x.name)
    val length = fields.length
    
    for(i <- 0 to fields.length-1){
      val field = fields(i)
      val fieldtype = field.dataType
      val fieldName = field.name
      fieldtype match {
        case arrayType: ArrayType =>
          val fieldNamesExcludingArray = fieldNames.filter(_!=fieldName)
          val fieldNamesAndExplode = fieldNamesExcludingArray ++ Array(s"explode_outer($fieldName) as $fieldName")
         // val fieldNamesToSelect = (fieldNamesExcludingArray ++ Array(s"$fieldName.*"))
          val explodedDf = df.selectExpr(fieldNamesAndExplode:_*)
          return flattenDataframe(explodedDf)
        case structType: StructType =>
          val childFieldnames = structType.fieldNames.map(childname => fieldName +"."+childname)
          val newfieldNames = fieldNames.filter(_!= fieldName) ++ childFieldnames
          val renamedcols = newfieldNames.map(x => (col(x.toString()).as(x.toString().replace(".", "_"))))
         val explodedf = df.select(renamedcols:_*)
          return flattenDataframe(explodedf)
        case _ =>
      }
    }
    df
  }
  
  val flattendedJSON = flattenDataframe(jsonDF)
  display(flattendedJSON)

