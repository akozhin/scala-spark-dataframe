package com.github.akozhin.data.engineer.spark.dataframe

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

class BostonCrimeStat(spark: SparkSession, crimesCsv: String, offenceDictCsv: String) {
  import spark.implicits._

  val offenceDict = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(offenceDictCsv).cache()

  val crimes = spark
    .read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(crimesCsv)

  val crimesWithDistrict = crimes.na.drop("any", Seq("DISTRICT"))



  val crimesByMonths = crimesWithDistrict
    .groupBy($"DISTRICT", $"MONTH")
    .count()
    .orderBy($"DISTRICT", $"MONTH")

  val crimesByType = crimesWithDistrict
    .groupBy($"DISTRICT", $"OFFENSE_CODE")
    .count()

  crimesByType.show()

  //row_number
  val windowSpec  = Window.partitionBy("DISTRICT").orderBy( $"count".desc)

  crimesByType
    .withColumn("row_number", row_number().over(windowSpec))
    .filter($"row_number" < 4 )
    .drop("row_number")
    .drop("count")
    .groupBy($"DISTRICT")
      .agg( collect_list($"OFFENSE_CODE") )
    .map(r => (r.getString(0),r.getSeq[String](1).mkString(", ")))
    .show()
//
//  val crimesSQL = crimesByMonths.createOrReplaceTempView("crimesByMonths")
//
//  val crimesMonthly = spark
//    .sql("" +
//      "select DISTRICT, percentile_approx(count,0.5) as crimes_monthly, avg(count) as avg  " +
//      "from crimesByMonths group by DISTRICT order by DISTRICT asc")
//    .show(false)
//
//  val crimesAggCoordinates = crimesWithDistrict
//    .groupBy($"DISTRICT")
//    .agg(
//      avg("Lat").as("lat"),
//      avg("Long").as("lng")
//    ).show(false)

}
