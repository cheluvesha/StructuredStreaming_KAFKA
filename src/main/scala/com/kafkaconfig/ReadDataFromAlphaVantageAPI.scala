package com.kafkaconfig

import org.apache.log4j.Logger
import spray.json._

import scala.io.Source

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
      val content = Source.fromURL(url)
      val contentInString = content.mkString
      content.close()
      contentInString
    } catch {
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
