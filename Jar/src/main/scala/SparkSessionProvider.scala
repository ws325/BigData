package agh

import org.apache.spark.sql.SparkSession

trait SparkSessionProvider {

  val spark = SparkSession
    .builder
    .master("local[4]")
    .appName("spark-cruncher")
    .getOrCreate()
  def getSparkSession():SparkSession = {return spark}
}
