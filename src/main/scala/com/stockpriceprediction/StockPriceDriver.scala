package com.stockpriceprediction

import com.Utility.UtilityClass
import com.kafkaconfig.{KafkaProducer, ReadDataFromAlphaVantageAPI}
import org.apache.log4j.Logger
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType
}
import spray.json.JsValue

object StockPriceDriver extends App {
  val sparkSession = UtilityClass.createSparkSessionObj("stock price")
  val topic = System.getenv("SSTOPIC")
  val apiKey: String = System.getenv("APIKEY")
  val companyName: String = "GOOG"
  val url: String =
    "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol=" +
      companyName + "&apikey=" + apiKey
  val broker = System.getenv("BROKERSS")
  val groupId = System.getenv("GROUPIDSS")
  val logger = Logger.getLogger(getClass.getName)
  val stockPricePrediction = new StockPricePrediction(sparkSession)
  val schema = StructType(
    List(
      StructField("1. open", StringType),
      StructField("2. high", StringType),
      StructField("3. low", StringType),
      StructField("4. close", StringType),
      StructField("5. volume", StringType)
    )
  )

  /***
    * Calls the Required methods in order to get Stock Data and to parse the data to json as well
    * @return Map[String, JsValue]
    */
  def fetchDataFromAlphaVantageAPI(): Map[String, JsValue] = {
    logger.info("Fetching data from Api")
    val apiData = ReadDataFromAlphaVantageAPI.getApiContent(url)
    val jsonData = ReadDataFromAlphaVantageAPI.parseDataToJson(apiData)
    jsonData
  }

  /***
    * Calls the create producer method and sends data to kafka topic
    * @param jsonData Map[String, JsValue]
    * @return Int
    */
  def sendDataToKafkaTopic(jsonData: Map[String, JsValue]): Int = {
    logger.info("sending data to kafka topic")
    val producer = KafkaProducer.createProducer(broker)
    val status =
      KafkaProducer.sendingDataToKafkaTopic(topic, jsonData, producer)
    status
  }

  /***
    * Process the data which is consumed from kafka, renames the field and cast the fields to required data type
    * @return DataFrame
    */
  def processTheConsumedDataFromKafka(): DataFrame = {
    try {
      logger.info("processing the data from kafka")
      val kafkaStockDF = stockPricePrediction.readDataFromKafka(broker, topic)
      kafkaStockDF.printSchema()
      val valuesDF =
        kafkaStockDF.select(
          from_json(col("value").cast("string"), schema).alias("values")
        )
      valuesDF.printSchema()
      logger.info("Renaming the fields")
      val renameFieldsDF = valuesDF
        .select("values.*")
        .withColumnRenamed("1. open", "Open")
        .withColumnRenamed("2. high", "High")
        .withColumnRenamed("3. low", "Low")
        .withColumnRenamed("4. close", "Close")
        .withColumnRenamed("5. volume", "Volume")
      renameFieldsDF.printSchema()
      logger.info("casting the fields of DataFrame")
      val castRenamedDF = renameFieldsDF.select(
        col("Open").cast(DoubleType),
        col("High").cast(DoubleType),
        col("Low").cast(DoubleType),
        col("Close").cast(DoubleType),
        col("Volume").cast(DoubleType)
      )
      castRenamedDF
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlAnalysisException.printStackTrace())
        throw new Exception("Unable to process DataFrame")
    }
  }
  val jsonData = fetchDataFromAlphaVantageAPI()
  val status = sendDataToKafkaTopic(jsonData)

  if (status == 1) {
    println("Sent Data to kafka successfully")
  } else {
    println("Unable to send data to kafka")
  }
  val processedDF = processTheConsumedDataFromKafka()

}
