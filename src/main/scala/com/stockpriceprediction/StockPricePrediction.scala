package com.stockpriceprediction

import java.io.FileNotFoundException

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

/***
  * Performs Stock price prediction using python model
  * @param sparkSession SparkSession
  */
class StockPricePrediction(
    sparkSession: SparkSession,
    pathToSave: String,
    pyFile: String
) {
  val logger: Logger = Logger.getLogger(getClass.getName)

  /***
    * Reads data from kafka topic and creates the DataFrame
    * @param broker String
    * @param topic String
    * @return DataFrame
    */
  def readDataFromKafka(broker: String, topic: String): DataFrame = {
    try {
      logger.info("Reading Data From Kafka Source")
      val kafkaDF = sparkSession.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", broker)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
      kafkaDF
    } catch {
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception("null fields passed")
    }
  }

  /***
    * Pipes the RDD with Python file
    * @param predictDF DataFrame
    * @param pyFile string
    * @return RDD[String]
    */
  def runPythonCommand(predictDF: DataFrame, pyFile: String): RDD[String] = {
    logger.info("piping the data with python file")
    try {
      val command = "python3 " + pyFile
      val pricePredictionRDD = predictDF.rdd.coalesce(1).pipe(command)
      pricePredictionRDD
    } catch {
      case fileNotFoundException: FileNotFoundException =>
        logger.error(fileNotFoundException.printStackTrace())
        throw new Exception("please provide the python file")
      case exception: Exception =>
        logger.error(exception.printStackTrace())
        throw new Exception("Unable to Pipe data")
    }
  }

  /***
    * sends DataFrame to runPythonCommand to Pipe the data and applies the linear regression model to predict the price
    * @param predictDF DataFrame
    * @param pyFile String
    * @return DataFrame
    */
  def loadLinearRegression(predictDF: DataFrame, pyFile: String): DataFrame = {
    logger.info("Loading Model To Predict The Closed Price")
    try {
      var predictedPriceRDD: RDD[String] = null
      if (!predictDF.isEmpty) {
        predictedPriceRDD =
          runPythonCommand(predictDF.drop("Close", "Date"), pyFile)
      }
      val pricePredicted =
        predictedPriceRDD.collect().toList.map(price => price.toDouble)
      val predictedPriceDF = sparkSession.createDataFrame(
        predictDF.rdd.zipWithIndex.map {
          case (row, columnIndex) =>
            Row.fromSeq(row.toSeq :+ pricePredicted(columnIndex.toInt))
        },
        StructType(
          predictDF.schema.fields :+ StructField(
            "Predicted Close Price",
            DoubleType,
            nullable = false
          )
        )
      )
      predictedPriceDF
    } catch {
      case sqlAnalysisException: org.apache.spark.sql.AnalysisException =>
        logger.error(sqlAnalysisException.printStackTrace())
        throw new Exception("Please check the Schema")
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception("Null variable not initialized")
    }
  }

  /***
    * Passes DataFrame to loadLinearRegression method and writes predicted price DataFrame to S3
    * @param predictDF DataFrame
    * @param pathToSave String
    * @param pyFile String
    */
  def predictPrice(
      predictDF: DataFrame,
      pathToSave: String,
      pyFile: String
  ): Unit = {
    logger.info("calling regression method and writing data to specified path")
    try {
      val predictedPriceDF = loadLinearRegression(predictDF, pyFile)
      if (!predictedPriceDF.isEmpty) {
        predictedPriceDF.write
          .option("header", value = true)
          .mode(SaveMode.Overwrite)
          .csv("output")
      }
    } catch {
      case exception: Exception =>
        logger.error(exception.printStackTrace())
        throw new Exception("Unable to write data to Sink")
    }
  }

  /***
    * Micro batch processing
    * @param processedDF DataFrame
    */
  def writeDataToSourceByPredictingPrice(processedDF: DataFrame): Unit = {
    logger.info("Micro batch is started")
    val query = processedDF.writeStream
      .foreachBatch { (predictDF: DataFrame, batchId: Long) =>
        logger.info("Running for the batch: " + batchId)
        predictPrice(predictDF, pathToSave, pyFile)
      }
      .queryName("Stock Prediction Query")
      .option("checkpointLocation", "chk-point-dir")
      .trigger(Trigger.ProcessingTime("5 seconds"))
      .start()
    query.awaitTermination(50000)
    logger.info("Micro batch is ended")
  }
}
