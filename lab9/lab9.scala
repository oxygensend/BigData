// Databricks notebook source
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructType,ArrayType}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Column
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.explode_outer


val path = "dbfs:/FileStore/tables/Nested.json"
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

// COMMAND ----------

val df = spark.read.json(path)

display(df)

// COMMAND ----------

val df = spark.read.option("multiline","true").json(path)
display(df)

// COMMAND ----------

display(df.withColumn("Nowa_pathLinkInfo", $"pathLinkInfo".dropFields("alternateName", "captureSpecification")))

// COMMAND ----------

// MAGIC %md
// MAGIC The Scala foldLeft method can be used to iterate over a data structure and perform multiple operations on a Spark DataFrame.

// COMMAND ----------

val prices: Seq[Double] = Seq(1.5, 2.0, 2.5)

// COMMAND ----------

val sum = prices.foldLeft(0.0)(_ + _)
sum

// COMMAND ----------

val donuts: Seq[String] = Seq("Plain", "Strawberry", "Glazed")
println(s"All donuts = ${donuts.foldLeft("")((a, b) => a + b + " Donut ")}")


// COMMAND ----------

import org.apache.spark.sql.functions.regexp_replace

val sourceDF = Seq(
  ("  p a   b l o", "Paraguay"),
  ("Neymar", "B r    asil")
).toDF("name", "country")

val actualDF = Seq(
  "name",
  "country"
).foldLeft(sourceDF) { (memoDF, colName) =>
  memoDF.withColumn(
    colName,
    regexp_replace(col(colName), "\\s+", "")
  )
}

actualDF.show()

// COMMAND ----------

val sourceDF = Seq(
  ("funny", "joke")
).toDF("A b C", "de F")

sourceDF.show()

val actualDF = sourceDF
  .columns
  .foldLeft(sourceDF) { (memoDF, colName) =>
    memoDF
      .withColumnRenamed(
        colName,
        colName.toLowerCase().replace(" ", "_")
      )
  }

actualDF.show()

// COMMAND ----------

val excludedNestedFields = Map("pathLinkInfo" -> Array("alternateName","captureSpecification" ))



// COMMAND ----------

excludedNestedFields.foldLeft(df) {(k,v) =>
  df.withColumn("sds", k.dropFields(v))
}



// COMMAND ----------


