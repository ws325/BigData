// Databricks notebook source
// MAGIC %md
// MAGIC ## Jak działa partycjonowanie
// MAGIC 
// MAGIC 1. Rozpocznij z 8 partycjami.
// MAGIC 2. Uruchom kod.
// MAGIC 3. Otwórz **Spark UI**
// MAGIC 4. Sprawdź drugi job (czy są jakieś różnice pomięczy drugim)
// MAGIC 5. Sprawdź **Event Timeline**
// MAGIC 6. Sprawdzaj czas wykonania.
// MAGIC   * Uruchom kilka razy rzeby sprawdzić średni czas wykonania.
// MAGIC 
// MAGIC Powtórz z inną liczbą partycji
// MAGIC * 1 partycja
// MAGIC * 7 partycja
// MAGIC * 9 partycja
// MAGIC * 16 partycja
// MAGIC * 24 partycja
// MAGIC * 96 partycja
// MAGIC * 200 partycja
// MAGIC * 4000 partycja
// MAGIC 
// MAGIC Zastąp `repartition(n)` z `coalesce(n)` używając:
// MAGIC * 6 partycji
// MAGIC * 5 partycji
// MAGIC * 4 partycji
// MAGIC * 3 partycji
// MAGIC * 2 partycji
// MAGIC * 1 partycji
// MAGIC 
// MAGIC ** *Note:* ** *Dane muszą być wystarczająco duże żeby zaobserwować duże różnice z małymi partycjami.*<br/>* To co możesz sprawdzić jak zachowują się małe dane z dużą ilośćia partycji.*

// COMMAND ----------

// val slots = sc.defaultParallelism
spark.conf.get("spark.sql.shuffle.partitions")

// COMMAND ----------

import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

val schema = StructType(
  List(
    StructField("timestamp", StringType, false),
    StructField("site", StringType, false),
    StructField("requests", IntegerType, false)
  )
)

val fileName = "dbfs:/databricks-datasets/wikipedia-datasets/data-001/pageviews/raw/pageviews_by_second.tsv"
val initialDF = spark.read
  .option("header", "true")
  .option("sep", "\t")
  .schema(schema)
  .csv(fileName)

// COMMAND ----------

initialDF.write.parquet("pageviews/raw/pageviews_by_second.parquet")

// COMMAND ----------

 spark.catalog.clearCache()
val parquetDir = "/pageviews/raw/pageviews_by_second.parquet"

val df = spark.read
  .parquet(parquetDir)
//.repartition(4000)
    .coalesce(1)
.groupBy("site").sum()


df.explain
df.count()
//printRecordsPerPartition(df)

// COMMAND ----------

df.rdd.getNumPartitions

// COMMAND ----------

//Repartition
//8 partycja - 2s
//1 partycja - 2s
//7 partycja - 2s
//9 partycja - 3s
//16 partycja - 2s
//24 partycja - 3s
//96 partycja - 5s
//200 partycja -8s
//4000 partycja -200s


//Coalesce
//     6 partycji - 4s
//     5 partycji - 4s
//     4 partycji - 3s
//     3 partycji - 4s
//     2 partycji - 3s
//     1 partycji - 4s

// COMMAND ----------

//Zadanie 2
def read_file(filePath:String): DataFrame = if(Files.exists(Paths.get(filePath))) {
    return spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)
  } else{
  println("No file found")
    return spark.emptyDataFrame
  }

 def to_feet(col_name: String): DataFrame ={
    if(df.columns.contains(col_name)){
     if (df.schema(col_name).dataType.typeName == "float"){
       this.df.withColumn(col_name, col(col_name.cast("Float"))/30.48)
     }
   else{
     this.df.withColumn(col_name, col(col_name)/30.48)
   }    
  }
    else{
      println("Column not in dataframe")
      return spark.emptyDataFrame
    }
  }

def clean_dataframe(col_names: String, df: String ) DataFrame ={
  for( i <- col_names){
   if(df.columns.contains(i))
      spark.sql(s"DELETE FROM $db.$i")
    else{
      println("Column not in dataframe")
      return spark.emptyDataFrame
    } 
  } 
  }
  
  def fill_na(col_names: String, df: String ) DataFrame ={
  for( i <- col_names){
   if(!df.columns.contains(i)){
      println("Column not in dataframe")
      return spark.emptyDataFrame
    } 
    df.na.fill("")
  } 
  }

 def most_frequent_value(col_name: String): DataFrame ={
    if(df.columns.contains(col_name)){
     return df.select(col(col_name)).withColumn("x", split(col(col_name), " ").getItem(0)).groupBy("x").count().orderBy(desc("count")) 
  }
    else{
      println("Column not in dataframe")
      return spark.emptyDataFrame
    }
  }

 def drop_col(col_name: String): DataFrame ={
    if(df.columns.contains(col_name)){
     return namesDf.drop(col(col_name))
  }
    else{
      println("Column not in dataframe")
      return spark.emptyDataFrame
    }
  }
