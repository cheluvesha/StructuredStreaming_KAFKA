package com.stockpriceTest

import com.Utility.UtilityClass
import com.stockpriceprediction.StockPriceDriver
import org.apache.spark.ml.feature.Intercept
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{StringType, StructField}
import org.scalatest.FunSuite

class StockPricePredictionTest extends FunSuite {
  val spark: SparkSession = UtilityClass.createSparkSessionObj("Test")
  val topic = "testingSS"
  val apiKey: String = System.getenv("APIKEY")
  val companyName: String = "GOOG"
  val url: String =
    "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol=" +
      companyName + "&apikey=" + apiKey
  val broker: String = System.getenv("BROKER_SS")
  val groupId = "testGrp1"
  val pyFile: String = System.getenv("PY_FILE")
  val schema = List(
    StructField("1. open", StringType),
    StructField("2. high", StringType),
    StructField("3. low", StringType),
    StructField("4. close", StringType),
    StructField("5. volume", StringType)
  )
  val wrongUrl = "http://qwfWAQGqeg/"

  test("givenURLItMustReadTheResponseAndParseTheData") {
    val jsonData = StockPriceDriver.fetchDataFromAlphaVantageAPI(url)
    assert(jsonData.nonEmpty)
  }

  test("givenWrongURLItMustThrowAnException") {
    val thrown = intercept[Exception] {
      StockPriceDriver.fetchDataFromAlphaVantageAPI(wrongUrl)
    }
    assert(thrown.getMessage === "Error While Retrieving The Data")
  }
}
