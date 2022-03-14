// Databricks notebook source
// MAGIC %scala
// MAGIC val actorsUrl = "https://raw.githubusercontent.com/cegladanych/azure_bi_data/main/IMDB_movies/actors.csv"
// MAGIC val actorsFile = "actors.csv"

// COMMAND ----------

// zad 1
import org.apache.spark.sql.types.{IntegerType, StringType, StructType, StructField}

val schema = StructType(Array(
  StructField("imdb_title_id", StringType, true),
  StructField("ordering", IntegerType, true),
  StructField("imdb_name_id", StringType, true),
  StructField("category", StringType, true),
  StructField("job", StringType, true),
  StructField("characters", StringType, true)))

val filePath = "dbfs:/FileStore/tables/Files/actors.csv"

val actorsDf = spark.read.format("csv")
            .option("header", "true")
            .schema(schema) //.option("inferSchema", "true") wczytanie z defaultowym schematem
            .load(filePath)

display(actorsDf)
actorsDf.printSchema


// COMMAND ----------

// zad 2
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.DataFrame

val dataRows = """
    [{
    "imdb_title_id": "tt0000009",
    "ordering": 1,
    "imdb_name_id": "nm0063086",
    "category": "actress",
    "job": "null",
    "characters": [
      "Miss Geraldine Holbrook (Miss Jerry)"
      ]
  },
  {
    "imdb_title_id": "tt0000574",
    "ordering": 1,
    "imdb_name_id": "nm0846887",
    "category": "actress",
    "job": "null",
    "characters": [
      "Kate Kelly"
      ]
  },
  {
    "imdb_title_id": "tt0000574",
    "ordering": 3,
    "imdb_name_id": "nm3002376",
    "category": "actor",
    "job": "null",
    "characters": [
      "Steve Hart"
      ]
  }]
  """

val schema = StructType(Array(
  StructField("imdb_title_id", StringType, true),
  StructField("ordering", IntegerType, true),
  StructField("imdb_name_id", StringType, true),
  StructField("category", StringType, true),
  StructField("job", StringType, true),
  StructField("characters", StringType, true)))

val json_to_df = spark.read.schema(schema).json(sc.parallelize(Array(dataRows)))

display(json_to_df)

val df_to_json_file = json_to_df.write.mode("overwrite").json("dbfs:/FileStore/tables/Files/DfJSON_2.json")

// COMMAND ----------

// zad 3
import org.apache.spark.sql.functions._

val kolumny = Seq("timestamp","unix", "Date")
val dane = Seq(("2015-03-22T14:13:34", 1646641525847L,"May, 2021"),
               ("2015-03-22T15:03:18", 1646641557555L,"Mar, 2021"),
               ("2015-03-22T14:38:39", 1646641578622L,"Jan, 2021"))

var dataFrame = spark.createDataFrame(dane).toDF(kolumny:_*)
  .withColumn("current_date",current_date().as("current_date"))
  .withColumn("current_timestamp",current_timestamp().as("current_timestamp"))
display(dataFrame)

// COMMAND ----------

// zad 3
dataFrame.printSchema()

// COMMAND ----------

// zad 3
val nowyunix = dataFrame.select($"timestamp",unix_timestamp($"timestamp","yyyy-MM-dd'T'HH:mm:ss").cast("timestamp")).show()

// COMMAND ----------

// zad 3
val tempE = dataFrame
  .withColumnRenamed("timestamp", "xxxx")
  .select( $"*", unix_timestamp($"xxxx", "yyyy-MM-dd'T'HH:mm:ss").cast("timestamp") )
  .withColumnRenamed("CAST(unix_timestamp(capturedAt, yyyy-MM-dd'T'HH:mm:ss) AS TIMESTAMP)", "xxxcast")


// COMMAND ----------

// zad 4
val data_badRecordsPath = spark.read.format("csv")
.option("inferSchema", "true")
.option("badRecordsPath", "dbfs:/FileStore/tables/Files/badrecords")
.load("dbfs:/FileStore/tables/Files/actors.csv")

val data_PERMISSIVE = spark.read.format("csv")
.option("inferSchema","true")
.option("mode", "PERMISSIVE")
.load("dbfs:/FileStore/tables/Files/actors.csv")
  
val data_DROPMALFORMED = spark.read.format("csv")
.option("inferSchema","true")
.option("mode", "DROPMALFORMED")
.load("dbfs:/FileStore/tables/Files/actors.csv")

val data_FAILFAST = spark.read.format("csv")
.option("inferSchema","true")
.option("mode", "FAILFAST")
.load("dbfs:/FileStore/tables/Files/actors.csv")


// COMMAND ----------

// zad 5
val to_parquet = actorsDf.write.mode("overwrite").option("header", "true").parquet("dbfs:/FileStore/tables/Files/parquet_file.parquet")

val to_json = actorsDf.write.mode("overwrite").option("header", "true").json("dbfs:/FileStore/tables/Files/json_file.json")

val from_parquet = spark.read.format("parquet")
.option("inferSchema","true")
.load("dbfs:/FileStore/tables/Files/parquet_file.parquet")

display(from_parquet)

val from_json = spark.read.format("json")
.option("inferSchema","true")
.load("dbfs:/FileStore/tables/Files/json_file.json")

// display(from_json)




