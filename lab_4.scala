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

//INFORMATION_SCHEMA.TABLES

val jdbcHostname = "bidevtestserver.database.windows.net"
val jdbcPort = 1433
val jdbcDatabase = "testdb"

val tabela = spark.read
  .format("jdbc")
  .option("url",s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user","sqladmin")
  .option("password","$3bFHs56&o123$")
  .option("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query","SELECT * FROM INFORMATION_SCHEMA.TABLES")
  .load()
display(tabela)

// COMMAND ----------

// MAGIC %md
// MAGIC 1. Pobierz wszystkie tabele z schematu SalesLt i zapisz lokalnie bez modyfikacji w formacie delta

// COMMAND ----------

val filteredTab = tabela.where("TABLE_SCHEMA == 'SalesLT'")
display(filteredTab)

// COMMAND ----------

val names_list = filteredTab.select("TABLE_NAME").as[String].collect.toList
print(names_list)

// COMMAND ----------

for( i <- names_list ){
  val tab = spark.read
  .format("jdbc")
  .option("url", s"jdbc:sqlserver://${jdbcHostname}:${jdbcPort};database=${jdbcDatabase}")
  .option("user", "sqladmin")
  .option("password", "$3bFHs56&o123$")
  .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver")
  .option("query", s"SELECT * FROM SalesLT.$i")
  .load()
  
  tab.write.format("delta").mode("overwrite").saveAsTable(i)  
}

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

//W każdej z tabel sprawdź ile jest nulls w rzędach i kolumnach

// COMMAND ----------

import org.apache.spark.sql.functions.{col, when, count}
import org.apache.spark.sql.Column

// COMMAND ----------

//funkcja do zlicznaia nulli w kolumnach
def colsNulls(columns: Array[String]): Array[Column]={
    columns.map(c=>{
      count(when(col(c).isNull,c)).alias(c)
    })
}

// COMMAND ----------

var i = List()
for( i <- names_list){
  val temp = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/"+i.toLowerCase())
temp.select(colsNulls(temp.columns):_*).show(false)
}

// COMMAND ----------

// Użyj funkcji fill żeby dodać wartości nie występujące w kolumnach dla wszystkich tabel z null

for( i <- names_list ){
  val temp = spark.read.format("delta")
  .option("header", "true")
  .load("dbfs:/user/hive/warehouse/"+i.toLowerCase())
  val tab = temp.na.fill("0", temp.columns).show(false)
}


// COMMAND ----------

//Użyj funkcji drop żeby usunąć nulle
for( i <- names_list ){
  val tab = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/"+i.toLowerCase())
  val tab_dropped = tab.na.drop().show(false)
}

// COMMAND ----------

//wybierz 3 dowolne funkcje agregujące i policz dla TaxAmt, Freight, [SalesLT].[SalesOrderHeader]


// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------


val sales_df = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/salesorderheader")
display(sales_df)


// COMMAND ----------

sales_df.select(mean($"TaxAmt").as("mean")).show()
sales_df.select(stddev($"Freight").as("std")).show()
sales_df.select(sum($"Freight").as("sum")).show()

// COMMAND ----------

//Użyj tabeli [SalesLT].[Product] i pogrupuj według ProductModelId, Color i ProductCategoryID i wylicz 3 wybrane funkcje agg()
val product_df = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/product")
val groupped_df = product_df.groupBy("ProductModelId", "Color", "ProductCategoryId").count()

groupped_df.select(avg($"ProductModelId").as("avg")).show()
groupped_df.select(min($"Color").as("min")).show()
groupped_df.select(max($"ProductCategoryId").as("max")).show()

// COMMAND ----------

display(product_df)

// COMMAND ----------

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.udf

// COMMAND ----------

//Stwórz 3 funkcje UDF do wybranego zestawu danych

val sum_val = udf((x: Int, y: Int) => (x+y))
spark.udf.register("sum_val", sum_val)

val mean_val = udf((x: Double, y: Double) => (x+y)/2)
spark.udf.register("mean_val", mean_val)

val num_val = udf((s: String) => (s.replaceAll("-", "").mkString("")))
spark.udf.register("num_val", num_val)

// COMMAND ----------

val test_df = spark.read.format("delta").option("header", "true").load("dbfs:/user/hive/warehouse/salesorderheader")

// COMMAND ----------

display(test_df)

// COMMAND ----------

val mean_test = test_df.select(mean_val($"TaxAmt", $"Freight") as("mean"))

display(mean_test)

// COMMAND ----------

val sum_test = test_df.select(sum_val($"TaxAmt", $"Freight") as("sum"))

display(sum_test)

// COMMAND ----------

val num_test = test_df.select(num_val($"AccountNumber"))

display(num_test)

