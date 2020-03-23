package com.github.akozhin.data.engineer.spark.dataframe

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * Use this to test the app locally, from sbt:
  * sbt "run inputFile.txt outputFile.txt"
  *  (+ select CountingLocalApp when prompted)
  */
object CountingLocalApp extends App{
  val (crimesCsv, offenceDictCsv, outputFolder) = (args(0), args(1), args(2))
  val spark = SparkSession.builder().master("local[*]").appName(this.getClass.getSimpleName).getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  val crimeStat = new BostonCrimeStat(spark,crimesCsv, offenceDictCsv);

}
