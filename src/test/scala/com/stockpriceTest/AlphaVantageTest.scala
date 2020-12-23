package com.stockpriceTest

import com.Utility.UtilityClass
import com.kafkaconfig.ReadDataFromAlphaVantageAPI
import com.kafkaconfig.ReadDataFromAlphaVantageAPI.parseDataToJson
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class AlphaVantageTest extends FunSuite {
  val sparkSession: SparkSession = SparkSession
    .builder()
    .master("local[*]")
    .appName("Test")
    .getOrCreate()
  val companyName = "GOOG"
  val apiKey: String = System.getenv("API_KEY")
  val url: String =
    "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol=" +
      companyName + "&apikey=" + apiKey
  val wrongUrl = "http://qwfWAQGqeg/"

  test("givenSparkSessionObjectWhenReturnedTypeMustEqualToActual") {
    val spark: SparkSession = UtilityClass.createSparkSessionObj("Test")
    assert(sparkSession === spark)
  }
  test("givenSparkSessionObjectWhenReturnedTypeMustNotEqualToActual") {
    val spark: SparkSession = UtilityClass.createSparkSessionObj("Test")
    assert(spark != null)
  }
  test("givenWhenUrlShouldRespondWithProperContent") {
    val response = ReadDataFromAlphaVantageAPI.getApiContent(url)
    assert(response != null)
  }
  test("givenWhenWrongUrlShouldRespondWithHttpException") {
    val response = intercept[Exception] {
      ReadDataFromAlphaVantageAPI.getApiContent(wrongUrl)
    }
    assert(response.getMessage === "Error While Retrieving The Data")
  }

  test("givenWhenDataShouldParseAndReturnMapValue") {
    val response = ReadDataFromAlphaVantageAPI.getApiContent(url)
    val jsMap = parseDataToJson(response)
    assert(jsMap != null)
  }

  test("givenWhenDataShouldParseAndItHasToContainValue") {
    val response = ReadDataFromAlphaVantageAPI.getApiContent(url)
    val jsMap = parseDataToJson(response)
    assert(jsMap != null)
  }

  test("givenWrongJsonFormatShouldThrowAnException") {
    val response = ""
    val thrown = intercept[Exception] {
      parseDataToJson(response)
    }
    assert(thrown.getMessage === "Error in Parsing Json Data")
  }
}
