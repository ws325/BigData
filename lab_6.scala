// Databricks notebook source
// MAGIC %md
// MAGIC Zad. 1<br>
// MAGIC Wykorzystaj przykłady z notatnika Windowed Aggregate Functions i przepisz funkcje używając Spark API

// COMMAND ----------

spark.sql("Create database if not exists Sample")


// COMMAND ----------

val transactions_col_names = Seq("AccountId","TranDate", "TranAmt")
val logical_col_names = Seq("RowID","FName", "Salary")

// COMMAND ----------

val Transactions = Seq(( 1, "2011-01-01", 500),
( 1, "2011-01-15", 50),
( 1, "2011-01-22", 250),
( 1, "2011-01-24", 75),
( 1, "2011-01-26", 125),
( 1, "2011-01-28", 175),
( 2, "2011-01-01", 500),
( 2, "2011-01-15", 50),
( 2, "2011-01-22", 25),
( 2, "2011-01-23", 125),
( 2, "2011-01-26", 200),
( 2, "2011-01-29", 250),
( 3, "2011-01-01", 500),
( 3, "2011-01-15", 50 ),
( 3, "2011-01-22", 5000),
( 3, "2011-01-25", 550),
( 3, "2011-01-27", 95 ),
( 3, "2011-01-30", 2500)).toDF(transactions_col_names:_*)

display(Transactions)


// COMMAND ----------

val Logical = Seq((1,"George", 800),
(2,"Sam", 950),
(3,"Diane", 1100),
(4,"Nicholas", 1250),
(5,"Samuel", 1250),
(6,"Patricia", 1300),
(7,"Brian", 1500),
(8,"Thomas", 1600),
(9,"Fran", 2450),
(10,"Debbie", 2850),
(11,"Mark", 2975),
(12,"James", 3000),
(13,"Cynthia", 3000),
(14,"Christopher", 5000)).toDF(logical_col_names:_*)

display(Logical)

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

// COMMAND ----------

//Totals based on previous row

// COMMAND ----------

val window  = Window.partitionBy("AccountId").orderBy("TranDate")
Transactions.withColumn("RunTotalAmt",sum("TranAmt").over(window)).orderBy("AccountId", "TranDate").show()

// COMMAND ----------

Transactions.withColumn("RunAvg",avg("TranAmt").over(window))
.withColumn("RunTranQty",count("*").over(window))
.withColumn("RunSmallAmt",min("TranAmt").over(window))
.withColumn("RunLargeAmt",max("TranAmt").over(window))
.withColumn("RunTotalAmt",sum("TranAmt").over(window))
.orderBy("AccountId", "TranDate").show()

// COMMAND ----------

//Calculating Totals Based Upon a Subset of Rows

// COMMAND ----------

val window_2 = Window.partitionBy("AccountId").orderBy("TranDate").rowsBetween(-2,Window.currentRow)

// COMMAND ----------

Transactions.withColumn("SlideAvg",avg("TranAmt").over(window_2))
 .withColumn("SlideQty", count("*").over(window_2))
 .withColumn("SlideMin", min("TranAmt").over(window_2))
 .withColumn("SlideMax", max("TranAmt").over(window_2))
 .withColumn("SlideTotal", sum("TranAmt").over(window_2))
 .withColumn("RN", row_number().over(window))
 .orderBy("AccountId", "TranDate").show()

// COMMAND ----------

//Logical Window

// COMMAND ----------

val window_3 = Window.orderBy("Salary").rowsBetween(Window.unboundedPreceding, Window.currentRow)
val window_4 = Window.orderBy("Salary").rangeBetween(Window.unboundedPreceding, Window.currentRow)

// COMMAND ----------

Logical.withColumn("SumByRows",sum("Salary").over(window_3))
       .withColumn("SumByRange",sum("Salary").over(window_4))
       .orderBy("RowID").show()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 2<br>
// MAGIC Użyj ostatnich danych i użyj funkcji okienkowych LEAD, LAG, FIRST_VALUE, LAST_VALUE, ROW_NUMBER i DENS_RANK
// MAGIC Każdą z funkcji wykonaj dla ROWS i RANGE i BETWEEN

// COMMAND ----------

val window = Window.partitionBy("AccountId").orderBy("TranAmt")
val windowRows =Window.partitionBy("AccountId").orderBy("TranAmt").rowsBetween(Window.unboundedPreceding, -2)
val windowRange = Window.partitionBy("AccountId").orderBy("TranAmt").rangeBetween(-1, Window.currentRow)

// COMMAND ----------

Transactions.withColumn("Lead", lead(col("TranAmt"),1).over(window))
.withColumn("Lag", lag(col("TranAmt"),1).over(window))
.withColumn("FirstRange", first("TranAmt").over(windowRange))
.withColumn("FirstRows", first("TranAmt").over(windowRows))
.withColumn("LastRange", last("TranAmt").over(windowRange))
.withColumn("LastRows", last("TranAmt").over(windowRows))
.withColumn("RowNumber",row_number().over(window))
.withColumn("DenseRank",dense_rank().over(window)).show()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 3<br>
// MAGIC Użyj ostatnich danych i wykonaj połączenia Left Semi Join, Left Anti Join, za każdym razem sprawdź .explain i zobacz jak spark wykonuje połączenia. Jeśli nie będzie danych to trzeba je zmodyfikować żeby zobaczyć efekty.

// COMMAND ----------

val DetailDf = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderdetail")

// COMMAND ----------

val HeaderDf = spark.read.format("delta")
              .option("header","true")
              .option("inferSchema","true")
              .load(s"dbfs:/user/hive/warehouse/salesorderheader")
display(HeaderDf)

// COMMAND ----------

val semi_join = HeaderDf.join(DetailDf, HeaderDf.col("SalesOrderID") === DetailDf.col("SalesOrderID"), "leftsemi")
display(semi_join)

// COMMAND ----------

semi_join.explain()

// COMMAND ----------

val anti_join = HeaderDf.join(DetailDf, HeaderDf("SalesOrderID") ===  DetailDf("SalesOrderID"),"leftanti")
display(anti_join)


// COMMAND ----------

anti_join.explain()

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 4<br>
// MAGIC Połącz tabele po tych samych kolumnach i użyj dwóch metod na usunięcie duplikatów. 

// COMMAND ----------

//pierwsza metoda
val temp = HeaderDf.join(DetailDf, HeaderDf.col("SalesOrderID") === DetailDf.col("SalesOrderID")).
drop(DetailDf.col("SalesOrderID"))
display(temp)

// COMMAND ----------

//druga metoda
val temp = HeaderDf.join(DetailDf, HeaderDf.col("SalesOrderID") === DetailDf.col("SalesOrderID"), "leftsemi").dropDuplicates("SalesOrderID")
display(temp)
// display(temp.select("SalesOrderID").dropDuplicates())

// COMMAND ----------

// MAGIC %md
// MAGIC Zadanie 5<br>
// MAGIC W jednym z połączeń wykonaj broadcast join, i sprawdź planie wykonania.

// COMMAND ----------

import org.apache.spark.sql.functions.broadcast

// COMMAND ----------

val broadcast_ = HeaderDf.join(broadcast(DetailDf), HeaderDf.col("SalesOrderID") === DetailDf.col("SalesOrderID"))
broadcast_.explain()
