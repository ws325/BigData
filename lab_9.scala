// Databricks notebook source
//Użycie funkcji dropFields i danych Nested.json
Podpowiedź:  withColumn(„nowacol”, $”konlj”.dropfiels(
 
//2a
//Sprawdź, jak działa ‘foldLeft’ i przerób kilka przykładów z internetu.


// COMMAND ----------

val nested_json = spark.read.format("json")
                         .option("inferSchema", "true")
                         .option("multiLine", "true")
                         .load("/FileStore/tables/Nested.json")
display(nested_json)

// COMMAND ----------

display(nested_json.withColumn("nowacol", $"pathLinkInfo".dropFields("alternateName", "elevationInDirection")))

// COMMAND ----------

// MAGIC %md
// MAGIC FoldLeft- Metoda może określać dowolną liczbę list parametrów. W sytuacji, kiedy jest ona wywołana dla mniejszej liczby list parametrów niż zostało to zdefiniowane, zwracana jest funkcja oczekująca pozostałych list parametrów jako argumentów.

// COMMAND ----------

val numbers = List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
val res = numbers.foldLeft(0)((m, n) => m + n)
println(res)

// COMMAND ----------

// MAGIC %md
// MAGIC wyrażenie numbers.foldLeft(0)(_ + _) pozwala trwale ustawić parametr z i przekazywać dalej częściową funkcję (partial function)

// COMMAND ----------

List(1, 2, 3, 4, 5).foldLeft(0)(_ + _)

// COMMAND ----------

List(1, 2, 3, 4, 5).foldLeft(5)(_ + _)

// COMMAND ----------

val parallelNumSeq = List(1, 2, 3, 4, 5).par

val foldLeftResult =
  parallelNumSeq.foldLeft(0) { (acc, currNum) =>
    val sum = acc + currNum
    println(s"FoldLeft: acc($acc) + currNum($currNum) = $sum ")
    sum
  }
println(foldLeftResult)

// COMMAND ----------

case class Person(name: String, sex: String)
val persons = List(Person("Thomas", "male"), Person("Sowell", "male"), Person("Liz", "female"))
val foldedList = persons.foldLeft(List[String]()) { (accumulator, person) =>
  val title = person.sex match {
    case "male" => "Mr."
    case "female" => "Ms."
  }
  accumulator :+ s"$title ${person.name}"
}
assert(foldedList == List("Mr. Thomas", "Mr. Sowell", "Ms. Liz"))
