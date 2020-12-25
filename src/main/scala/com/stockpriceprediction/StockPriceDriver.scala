package com.stockpriceprediction

import com.Utility.UtilityClass
import com.kafkaconfig.{KafkaProducer, ReadDataFromAlphaVantageAPI}
import org.apache.log4j.Logger
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions.{col, from_json}
import org.apache.spark.sql.types.{
  DoubleType,
  StringType,
  StructField,
  StructType,
  TimestampType
}
import spray.json.JsValue

object StockPriceDriver {
  val sparkSession: SparkSession =
    UtilityClass.createSparkSessionObj("stock price")
  val topic: String = System.getenv("TOPIC_SS")
  val apiKey: String = System.getenv("API_KEY")
  val companyName: String = "GOOG"
  val url: String =
    "https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY&interval=1min&symbol=" +
      companyName + "&apikey=" + apiKey
  val broker: String = System.getenv("BROKER_SS")
  val groupId: String = System.getenv("GROUP_ID_SS")
  val logger: Logger = Logger.getLogger(getClass.getName)
  val pathToSave = "x"
  val pyFile: String = System.getenv("PY_FILE")
  val stockPricePrediction =
    new StockPricePrediction(sparkSession, pathToSave, pyFile)
  val schema = List(
    StructField("1. open", StringType),
    StructField("2. high", StringType),
    StructField("3. low", StringType),
    StructField("4. close", StringType),
    StructField("5. volume", StringType)
  )

  /***
    * Calls the Required methods in order to get Stock Data and to parse the data to json as well
    * @return Map[String, JsValue]
    */
  def fetchDataFromAlphaVantageAPI(url: String): Map[String, JsValue] = {
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
  def sendDataToKafkaTopic(
      jsonData: Map[String, JsValue],
      broker: String,
      topic: String
  ): Int = {
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
  def processTheConsumedDataFromKafka(
      broker: String,
      topic: String,
      schema: List[StructField]
  ): DataFrame = {
    try {
      logger.info("processing the data from kafka")
      val kafkaStockDF = stockPricePrediction.readDataFromKafka(broker, topic)
      kafkaStockDF.printSchema()
      val valuesDF =
        kafkaStockDF
          .select(
            from_json(col("value").cast("string"), StructType(schema)).alias(
              "values"
            ),
            col("key").cast("string")
          )
          .selectExpr("values.*", "key")
          .withColumnRenamed("1. open", "Open")
          .withColumnRenamed("2. high", "High")
          .withColumnRenamed("3. low", "Low")
          .withColumnRenamed("4. close", "Close")
          .withColumnRenamed("5. volume", "Volume")
          .withColumnRenamed("key", "Date")
      logger.info("casting the fields of DataFrame")
      val castRenamedDF = valuesDF.select(
        col("Open").cast(DoubleType),
        col("High").cast(DoubleType),
        col("Low").cast(DoubleType),
        col("Close").cast(DoubleType),
        col("Volume").cast(DoubleType),
        col("Date").cast(TimestampType)
      )
      castRenamedDF
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlAnalysisException.printStackTrace())
        throw new Exception("Unable to process DataFrame")
    }
  }
  def main(args: Array[String]): Unit = {
    val jsonData = fetchDataFromAlphaVantageAPI(url)
    val status = sendDataToKafkaTopic(jsonData, broker, topic)

    if (status == 1) {
      println("Sent Data to kafka successfully")
    } else {
      println("Unable to send data to kafka")
    }
    val processedDF = processTheConsumedDataFromKafka(broker, topic, schema)
    processedDF.printSchema()
    stockPricePrediction.writeDataToSourceByPredictingPrice(processedDF)
  }
}
