package agh

import java.time.Year
import org.apache.spark.sql.functions._

object Movies extends SparkSessionProvider {

  def main(args: Array[String]): Unit = {
    val path = args(0)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(path)
      .na.drop()
      .withColumn("years_passed", (col("year").cast("int") - Year.now.getValue) * -1)

    df.show()
  }

}