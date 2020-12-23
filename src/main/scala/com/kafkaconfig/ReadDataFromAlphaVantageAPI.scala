package com.kafkaconfig

import org.apache.http.client.methods.HttpGet
import org.apache.http.impl.client.HttpClientBuilder
import org.apache.log4j.Logger
import spray.json._

/***
  * Class connects to Alpha Vantage Api to fetch http response
  */
object ReadDataFromAlphaVantageAPI {

  val logger: Logger = Logger.getLogger(getClass.getName)

  /***
    * method gets data from API in the form of http response
    * @param url String
    * @return String
    */
  def getApiContent(url: String): String = {
    try {
      logger.info("fetching data from API")
      val httpClient = HttpClientBuilder.create().build()
      val httpResponse = httpClient.execute(new HttpGet(url))
      logger.info("Result: " + httpResponse.getStatusLine.getStatusCode)
      val entity = httpResponse.getEntity
      var content: String = ""
      if (entity != null) {
        val inputStream = entity.getContent
        content = scala.io.Source.fromInputStream(inputStream).getLines.mkString
        inputStream.close()
      }
      httpClient.close()
      content
    } catch {
      case httpException: org.apache.http.conn.HttpHostConnectException =>
        logger.error(httpException.printStackTrace())
        throw new Exception("HTTP Connection Error")
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Error While Retrieving The Data")
    }
  }

  /***
    * method parse the String data to JSON using Spray Json
    * @param data String
    * @return Map[String, JsValue]
    */
  def parseDataToJson(data: String): Map[String, JsValue] = {
    try {
      logger.info("parsing data to json")
      val jsonStockData = data.parseJson
      val requiredData = jsonStockData.asJsObject.fields("Time Series (1min)")
      val requiredDataMap = requiredData.asJsObject.fields
      requiredDataMap
    } catch {
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Error in Parsing Json Data")
    }
  }
}
