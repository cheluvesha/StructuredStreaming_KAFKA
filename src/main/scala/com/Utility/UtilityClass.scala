package com.Utility

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/***
  *  Utility class provides useful methods
  */
object UtilityClass {

  /** *
    * creates SparkSession Object
    * @param appName String
    * @return SparkSession
    */
  def createSparkSessionObj(appName: String): SparkSession = {
    val sparkConfigurations = new SparkConf()
      .setAppName(appName)
      .setMaster("local[*]")
      .set("spark.streaming.kafka.maxRatePerPartition", "10")
      .set("spark.streaming.stopGracefullyOnShutdown", "true")

    val sparkSession = SparkSession
      .builder()
      .config(sparkConfigurations)
      .getOrCreate()
    sparkSession
  }
}
