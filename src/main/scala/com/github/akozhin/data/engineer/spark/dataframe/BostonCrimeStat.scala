package com.github.akozhin.data.engineer.spark.dataframe

import org.apache.spark.sql.functions.avg
import org.apache.spark.sql.{DataFrame, SparkSession}

class BostonCrimeStat(spark: SparkSession, crimesCsv: String, offenceDictCsv: String) {

  import spark.implicits._

  val crimes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(crimesCsv)
    .na.drop("any", Seq("DISTRICT"))
    .cache()

  val offenceDict = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(offenceDictCsv).cache()

  val crimesByMonths = crimes
    .groupBy($"DISTRICT", $"MONTH")
    .count()

  val crimesSQL = crimes.createOrReplaceTempView("crimesByMonths")

  val crimesMonthly = spark
    .sql("select DISTRICT, percentile_approx(MONTH,0.5) as crimes_monthly from crimesByMonths group by DISTRICT order by DISTRICT asc")

  val crimesAggCoordinates = crimes
    .groupBy($"DISTRICT")
    .agg(
      avg("Lat").as("lat"),
      avg("Long").as("lng")
    )

}
