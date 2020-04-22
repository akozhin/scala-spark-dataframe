package com.github.akozhin.data.engineer.spark.dataframe

import org.apache.avro.file.DataFileWriter
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object BostonCrimeStat {

  def calcAndSave(spark: SparkSession, crimesCsv: String, offenceDictCsv: String, outputPath: String) {
    import spark.implicits._

    val offenceDict = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(offenceDictCsv)
      .map(r => (r.getInt(0), r.getString(1).split("-")(0).trim()))
      .withColumnRenamed("_1", "CODE")
      .withColumnRenamed("_2", "crime_type")
      .distinct()
      .dropDuplicates("CODE") //херня
      .cache()

//    offenceDict.orderBy("_1")show(1000,false)

    val crimes = spark
      .read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(crimesCsv)

    val crimesWithDistrictNotNull = crimes.na.drop("any", Seq("DISTRICT"))

    val crimesFull = crimesWithDistrictNotNull
      .join(offenceDict, crimes("offense_code") === offenceDict("CODE"))

    val crimesByMonths = crimesWithDistrictNotNull
      .groupBy($"DISTRICT", $"MONTH")
      .count()
      .orderBy($"DISTRICT", $"MONTH")

//    crimesByMonths.show(false)

    //row_number
    val windowSpec = Window.partitionBy("DISTRICT").orderBy($"count".desc)

    val frequentCrimeTypes = crimesFull
      .groupBy($"DISTRICT", $"OFFENSE_CODE")
      .count()
      .withColumn("row_number", row_number().over(windowSpec))
      .filter($"row_number" < 4)
      .drop("row_number")
      .drop("count")
      .join(offenceDict, crimesFull("offense_code") === offenceDict("CODE"))
      .groupBy($"DISTRICT")
      .agg(collect_list($"crime_type"))
//      .show(1000,false)
      .map(r => (r.getString(0), r.getSeq[String](1).mkString(", ")))
      .withColumnRenamed("_1", "DISTRICT")
      .withColumnRenamed("_2", "frequent_crime_types")
//          .show(1000,false)

    val crimesSQL = crimesByMonths.createOrReplaceTempView("crimesByMonths")

    val crimesMonthly = spark
      .sql("" +
        "select DISTRICT, percentile_approx(count,0.5) as crimes_monthly " +
        "from crimesByMonths group by DISTRICT order by DISTRICT asc")

    crimesMonthly.show(false)

    val crimesTotalAvgLocation = crimesWithDistrictNotNull
      .groupBy(crimesWithDistrictNotNull("DISTRICT").as("district"))
      .agg(
        count("INCIDENT_NUMBER").as("crimes_total"),
        avg("Lat").as("lat"),
        avg("Long").as("lng")
      )

    val crimeStat = crimesTotalAvgLocation
      .join(crimesMonthly, crimesMonthly("DISTRICT") === crimesTotalAvgLocation("district"))
      .join(frequentCrimeTypes, frequentCrimeTypes("DISTRICT") === crimesTotalAvgLocation("district"))
      .drop(crimesMonthly("DISTRICT"))
      .drop(frequentCrimeTypes("DISTRICT"))
      .orderBy("district")

    crimeStat.show(false)
    println("Output path : " + outputPath)
    crimeStat.write.parquet(outputPath + "boston-crime-stat.parquet")
  }
}
