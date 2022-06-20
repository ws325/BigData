// Databricks notebook source
// MAGIC %md
// MAGIC 
// MAGIC Dane z servera Kafka pochodzą z Twittera
// MAGIC 
// MAGIC 
// MAGIC 0. Zmiejsz partycje shuffle do 4
// MAGIC 0. Typ streamu Kafka
// MAGIC 0. Lokalizacja serverów  **server1.databricks.training:9092** (US-Oregon) - **server2.databricks.training:9092** (Singapore)
// MAGIC 0. Topic "subscribe" to "tweets"
// MAGIC 0. Throttle Kafka's processing of the streams (maxOffsetsPerTrigger)
// MAGIC 0. Opcja przy ponownym uruchomieniu notatnika przewiń strumień do początku (startingOffsets)
// MAGIC 0. Załaduj dane
// MAGIC 0. Wybież kolumne `value` cast do typu `STRING`

// COMMAND ----------

spark.conf.set("spark.sql.shuffle.partitions", 4)

val kafkaServer = ""

val twittsDF = spark.readStream
  .format("kafka")
  .option("kafka.bootstrap.servers","server1.databricks.training:9092")
  .option("subscribe","tweets")
  .option("maxOffsetsPerTrigger",1000)
  .option("startingOffsets","earliest")
  .load()
  .select($"value".cast("string"))

// COMMAND ----------

// MAGIC %md
// MAGIC * Sprawdź czy działa

// COMMAND ----------

twittsDF.isStreaming

// COMMAND ----------

// MAGIC %md
// MAGIC Schemat danych

// COMMAND ----------


import org.apache.spark.sql.types.{StructType, StructField, StringType, IntegerType, LongType, ArrayType}

lazy val twitSchema = StructType(List(
  StructField("hashTags", ArrayType(StringType, false), true),
  StructField("text", StringType, true),
  StructField("userScreenName", StringType, true),
  StructField("id", LongType, true),
  StructField("createdAt", LongType, true),
  StructField("retweetCount", IntegerType, true),
  StructField("lang", StringType, true),
  StructField("favoriteCount", IntegerType, true),
  StructField("user", StringType, true),
  StructField("place", StructType(List(
  StructField("coordinates", StringType, true),
  StructField("name", StringType, true),
  StructField("placeType", StringType, true),
  StructField("fullName", StringType, true),
  StructField("countryCode", StringType, true)
  )), true)
))

// COMMAND ----------

// MAGIC %md
// MAGIC  JSON DataFrame
// MAGIC 
// MAGIC * Użyj `twittsDF` i sparsuj dane uzywając `from_json`.
// MAGIC * Stwórz DataFrame, z poniższymi polami
// MAGIC * `time` (już podany)
// MAGIC * Dodaj kolumnę `json`, która pochodzi z kolumny `value`
// MAGIC * Wypłaszcz (flatten) pola jakie wystąpią w kolumnie `json`

// COMMAND ----------

import org.apache.spark.sql.functions.{from_json, expr}

val DF = twittsDF
 .withColumn("json", from_json($"value", twitSchema))                         
 .select(
   expr("cast(cast(json.createdAt as double)/1000 as timestamp) as time"),
   $"json.hashTags".as("hashTags"),                                           
   $"json.text".as("text"),
   $"json.userScreenName".as("userScreenName"),
   $"json.id".as("id"),
   $"json.retweetCount".as("retweetCount"),
   $"json.lang".as("lang"),
   $"json.favoriteCount".as("favoriteCount"),
   $"json.user".as("user"),
   $"json.place.coordinates".as("coordinates"),
   $"json.place.name".as("name"),
   $"json.place.placeType".as("placeType"),
   $"json.place.fullName".as("fullName"),
   $"json.place.countryCode".as("countryCode")
 )

// COMMAND ----------

// MAGIC %md
// MAGIC * Wyświetl dane

// COMMAND ----------

display(DF)

// COMMAND ----------

// MAGIC %md
// MAGIC Zatrzymaj stream

// COMMAND ----------

for (x <- spark.streams.active)
  x.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC Obróbka hashtagów
// MAGIC 
// MAGIC * Dodaj kolumę 'hashTag', która podzieli kolumnę Hashtags na wiele wierszy
// MAGIC * Zmień wszystkie hashtagi do 'lower case'
// MAGIC * Grupuj po hashtagu i policz ile ich jest
// MAGIC * Posortuj dane po ilości malejąco
// MAGIC * wyciągnij 30 najpopularniejszych hashtagów

// COMMAND ----------

import org.apache.spark.sql.functions.{lower, explode, desc}

val najpopularniejszeHashtagiDF = DF.select(explode($"hashTags").as("hashTag"),lower($"hashTag"))
 .groupBy("hashTag")
 .count()
 .orderBy($"count".desc)
 .limit(30)

// COMMAND ----------

// MAGIC %md
// MAGIC Pokaż na wykresie wynik z najpopularniejszeHashtagiDF

// COMMAND ----------

display(najpopularniejszeHashtagiDF)

// COMMAND ----------

// MAGIC %md
// MAGIC * Wstrzymaj stream

// COMMAND ----------

for (x <- spark.streams.active)
  x.stop()

// COMMAND ----------

// MAGIC %md
// MAGIC Zapisz stream
// MAGIC * Użyj formatu tabeli sink jako `in-memory`
// MAGIC * Output mode "append"
// MAGIC * Nazwij query
// MAGIC * Skonfiguruj wyzwalacz - co 10 sekund
// MAGIC * Uruchom query

// COMMAND ----------

import org.apache.spark.sql.streaming.Trigger
val query = najpopularniejszeHashtagiDF.writeStream
    .format("memory")
    .outputMode("complete")
    .queryName("countHashTag")
    .trigger(Trigger.ProcessingTime("10 seconds"))
    .start()

// COMMAND ----------

// MAGIC %md
// MAGIC Wyłącz stream

// COMMAND ----------

for (x <- spark.streams.active)
  x.stop()
