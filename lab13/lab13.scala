// Databricks notebook source
val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val df = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

// COMMAND ----------

// DBTITLE 1,ZADANIE 2
//W zakładce Executors znajdzują się wszystkie informacje o dystrybucji danych pomiędzy executorów
//Compute, następnie w klastrze, w zakładce Metrics, pod live matrics, znajduje się Ganglia UI.

// COMMAND ----------

// DBTITLE 1,ZADANIE 3
// W trakcie tworzenia klastra można ustawić w Spark config ilość pamięci przydzielonej dla executor. Np: spark.executor.memory 1g 
// Umożliwia nam to kontrolę zurzywania pamięci

// COMMAND ----------

// DBTITLE 1,ZADANIE 4
df.write
  .mode("overwrite")
  .partitionBy("height")
  .format("parquet")
  .saveAsTable("partitionFiles")

df.write
  .mode("overwrite")
  .bucketBy(10,"height")
  .format("parquet")
  .saveAsTable("bucketFiles")

// COMMAND ----------

// Tak, będą różnice w plikach

// COMMAND ----------

// DBTITLE 1,ZADANIE 5
spark.sql("ANALYZE TABLE partitionfiles COMPUTE STATISTICS")
spark.sql("DESCRIBE EXTENDED partitionfiles")

// COMMAND ----------

// MAGIC %sql
// MAGIC ANALYZE TABLE partitionfiles COMPUTE STATISTICS;
// MAGIC DESCRIBE EXTENDED partitionfiles;

// COMMAND ----------

// MAGIC %sql
// MAGIC ANALYZE TABLE partitionfiles COMPUTE STATISTICS FOR ALL COLUMNS;
// MAGIC DESCRIBE EXTENDED partitionfiles;
