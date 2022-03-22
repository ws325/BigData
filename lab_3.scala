// Databricks notebook source
// MAGIC %md Names.csv 
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
// MAGIC * Odpowiedz na pytanie jakie jest najpopularniesze imię?
// MAGIC * Dodaj kolumnę i policz wiek aktorów 
// MAGIC * Usuń kolumny (bio, death_details)
// MAGIC * Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// MAGIC * Posortuj dataframe po imieniu rosnąco

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/names.csv"
val namesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(namesDf)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val epochDf = namesDf.withColumn("epoch", from_unixtime(unix_timestamp()))
display(epochDf)


// COMMAND ----------

//Dodaj kolumnę w której wyliczysz wzrost w stopach (feet)
val feetDf = epochDf.withColumn("height(feet)", $"height"/30.48)
display(feetDf)

// COMMAND ----------

//Odpowiedz na pytanie jakie jest najpopularniesze imię?
val pop_name = feetDf.select(split($"name", " ").getItem(0).as("split_name")).groupBy("split_name").count().orderBy(desc("count")).show(1)
//Najpopularniejsze imię to John


// COMMAND ----------

//Dodaj kolumnę i policz wiek aktorów
val tempDf = feetDf.withColumn("current_date",current_date()) //dodaje kolumne z aktualnym czasem, gdy aktor nadal żyje
 .withColumn("birth_date",to_date($"date_of_birth", "dd.MM.yyyy"))
 .withColumn("death_date",to_date($"date_of_death", "dd.MM.yyyy"))

//floor - returns the smallest integral number not greater than expr
val ageDf = tempDf.withColumn("age", when(col("death_date").isNull, floor(datediff($"birth_date",$"current_date")/(-365))).otherwise(floor(datediff($"birth_date",$"death_date")/(-365))))
display(ageDf)



// COMMAND ----------

//Usuń kolumny (bio, death_details)
// feetDf.printSchema() //przed usunięciem
val dropDf = ageDf.drop("bio").drop("death_details")
// display(dropDf)
dropDf.printSchema() //po usunięciu

// COMMAND ----------

//Zmień nazwy kolumn - dodaj kapitalizaję i usuń _
// new_column_name_list= list(map(lambda x: x.replace(" ", "_"), dropDf.columns))

// df = dropDf.toDF(*new_column_name_list)

// COMMAND ----------

//Posortuj dataframe po imieniu rosnąco
val sortNameDf = dropDf.sort("name")
display(sortNameDf)

// COMMAND ----------

// MAGIC %md Movies.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
// MAGIC * Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
// MAGIC * Usuń wiersze z dataframe gdzie wartości są null

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/movies.csv"
val moviesDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(moviesDf)

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val epochDf_2 = moviesDf.withColumn("epoch", from_unixtime(unix_timestamp()))
display(epochDf_2)

// COMMAND ----------

//Dodaj kolumnę która wylicza ile lat upłynęło od publikacji filmu
val tempDf_2 = epochDf_2.withColumn("current_year",current_date()).withColumn("current_year", year($"current_date"))
val yearsDf = tempDf_2.withColumn("years_publication", floor($"current_year"-$"year"))

display(yearsDf)

// COMMAND ----------

//Dodaj kolumnę która pokaże budżet filmu jako wartość numeryczną, (trzeba usunac znaki walut)
val budgetDf = yearsDf.withColumn("budget_without_$", regexp_replace(yearsDf("budget"), "\\D+", ""))

display(budgetDf)

// COMMAND ----------

//Usuń wiersze z dataframe gdzie wartości są null
val delNaDf = budgetDf.na.drop()
display(delNaDf)

// COMMAND ----------

// MAGIC %md ratings.csv
// MAGIC * Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
// MAGIC * Dla każdego z poniższych wyliczeń nie bierz pod uwagę `nulls` 
// MAGIC * Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10)
// MAGIC * Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote
// MAGIC * Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
// MAGIC * Dla jednej z kolumn zmień typ danych do `long` 

// COMMAND ----------

val filePath = "dbfs:/FileStore/tables/Files/ratings.csv"
val ratingsDf = spark.read.format("csv")
              .option("header","true")
              .option("inferSchema","true")
              .load(filePath)

display(ratingsDf)

// COMMAND ----------

import org.apache.spark.sql.functions._

// COMMAND ----------

//Dodaj kolumnę z wartością czasu wykonania notatnika w formacie epoch
val epochDf_3 = ratingsDf.withColumn("epoch", from_unixtime(unix_timestamp()))
display(epochDf_3)

// COMMAND ----------

//Dla każdego z poniższych wyliczeń nie bierz pod uwagę nulls
val ratingsWithoutNaDf = epochDf_3.na.drop()
display(epochDf_3)

// COMMAND ----------

//Dodaj nowe kolumny i policz mean i median dla wartości głosów (1 d 10) //
// val temp_val = ratingsWithoutNaDf.agg(avg("mean_vote") as "mean")
// display(temp_val)


// COMMAND ----------

//Dla każdej wartości mean i median policz jaka jest różnica między weighted_average_vote

// COMMAND ----------

//Kto daje lepsze oceny chłopcy czy dziewczyny dla całego setu
val mean_val = ratingsWithoutNaDf.select("females_allages_avg_vote","males_allages_avg_vote")
.agg(avg("females_allages_avg_vote") as "females",avg("males_allages_avg_vote") as "males")
display(mean_val)
//chłopcy dają lepsze oceny

// COMMAND ----------

//Dla jednej z kolumn zmień typ danych do long
import org.apache.spark.sql.types.{LongType}
val colToLongDf = ratingsWithoutNaDf.withColumn("total_votes(long)", col("total_votes").cast(LongType))


// COMMAND ----------

// Zad. 2

//SPARK UI:
// - Jobs Tab - informacje o statusie zadan
// - Stages Tab - stan wszystkich etapow dla zadan w Sparku
// - Storage Tab - prezentuje stan pamieci
// - Environment Tab - informacje o zmiennych srodowiskowych
// - Executors Tab - zawiera liste egzekutorow jobow i ich stan
// - SQL Tab - zawiera informacje na temat wywolan sql
// - Structured Streaming Tab - statystyki dotyczacych uruchomionych i zakończonych zapytan, ostatni wyjatek nieudanego zapytania
// - JDBC/ODBC Server Tab - pokazuje informacje o sesjach i przesłanych operacjach SQL


// COMMAND ----------

// Zad. 3
val explDf = namesDf.select($"name",$"height").explain()

val explgroupDF = namesDf.select($"name",$"height").groupBy("name").count().explain()

// COMMAND ----------

// Zad. 4
val tabDF = sqlContext.read
      .format("jdbc")
      .option("driver" , "com.microsoft.sqlserver.jdbc.SQLServerDriver")
      .option("url", "jdbc:sqlserver://bidevtestserver.database.windows.net:1433;database=testdb")
      .option("dbtable", "(SELECT * FROM information_schema.tables) tmp")
      .option("user", "sqladmin")
      .option("password", "$3bFHs56&o123$")
      .load()

display(tabDF)

