package com.stockpriceTest

import com.Utility.UtilityClass
import com.stockpriceprediction.StockPriceDriver
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import org.scalatest.FunSuite
import spray.json.JsValue

class StockPricePredictionTest extends FunSuite {
  val spark: SparkSession = UtilityClass.createSparkSessionObj("Test")
  val topic = "testingSS"
  val apiKey: String = System.getenv("APIKEY")
  val companyName: String = "GOOG"
  val url: String =
    "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol=" +
      companyName + "&apikey=" + apiKey
  val broker: String = System.getenv("BROKER")
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
  var jsonData: Map[String, JsValue] = _
  val wrongBroker: String = null
  val wrongTopic: String = null
  val dataFrameSchema: StructType = StructType(
    List(
      StructField("Open", DoubleType, nullable = true),
      StructField("High", DoubleType, nullable = true),
      StructField("Low", DoubleType, nullable = true),
      StructField("Close", DoubleType, nullable = true),
      StructField("Volume", DoubleType, nullable = true),
      StructField("Date", TimestampType, nullable = true)
    )
  )
  val wrongSchema = List(StructField("Open", DoubleType, nullable = true))

  test("givenURLItMustReadTheResponseAndParseTheData") {
    jsonData = StockPriceDriver.fetchDataFromAlphaVantageAPI(url)
    assert(jsonData.nonEmpty)
  }

  test("givenWrongURLItMustThrowAnException") {
    val thrown = intercept[Exception] {
      StockPriceDriver.fetchDataFromAlphaVantageAPI(wrongUrl)
    }
    assert(thrown.getMessage === "Error While Retrieving The Data")
  }
  test("givenDataItMustCreateProducerAndSendItToKafkaTopic") {
    val status = StockPriceDriver.sendDataToKafkaTopic(jsonData, broker, topic)
    assert(status === 1)
  }
  test("givenNullFieldsItMustThrowAnException") {
    val thrown = intercept[Exception] {
      StockPriceDriver.sendDataToKafkaTopic(jsonData, broker, topic)
    }
    assert(thrown.getMessage === "Broker data is null")
  }
  test("givenKafkaDetailsItMustReadTheDataAndItMustProcessTheData") {
    val castRenamedDF =
      StockPriceDriver.processTheConsumedDataFromKafka(broker, topic, schema)
    assert(castRenamedDF.schema === dataFrameSchema)
  }
  test("givenWrongKafkaDetailsItMustReadTheDataAndItMustThrowAnException") {
    val thrown = intercept[Exception] {
      StockPriceDriver.processTheConsumedDataFromKafka(
        wrongBroker,
        wrongTopic,
        schema
      )
    }
    assert(thrown.getMessage === "null fields passed")
  }
  test("givenWrongSchemaDetailsItMustReadTheDataAndItMustThrowAnException") {
    val thrown = intercept[Exception] {
      StockPriceDriver.processTheConsumedDataFromKafka(
        broker,
        topic,
        wrongSchema
      )
    }
    assert(thrown.getMessage === "Unable to process DataFrame")
  }
}
