package com.kafkaconfig

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.log4j.Logger
import spray.json.JsValue

/***
  * Producer Class responsible for sending data to Kafka topic
  */
object KafkaProducer {
  val logger: Logger = Logger.getLogger(getClass.getName)

  /***
    * Creates Kafka Producer
    * @param broker String
    * @return KafkaProducer[String, String]
    */
  def createProducer(broker: String): KafkaProducer[String, String] = {
    try {
      logger.info("creating the producer")
      val props = new Properties()
      props.put("bootstrap.servers", broker)
      props.put(
        "key.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      props.put(
        "value.serializer",
        "org.apache.kafka.common.serialization.StringSerializer"
      )
      val producer = new KafkaProducer[String, String](props)
      producer
    } catch {
      case kafkaException: org.apache.kafka.common.KafkaException =>
        logger.error(kafkaException.printStackTrace())
        throw new Exception("Unable to create kafka producer")
      case nullPointerException: NullPointerException =>
        logger.error(nullPointerException.printStackTrace())
        throw new Exception("Broker data is null")
    }
  }

  /***
    * Sends Data to Kafka Topic
    * @param topic String
    * @param dataToBePassed Map[String, JsValue]
    * @param kafkaProducer KafkaProducer[String, String]
    * @return Int
    */
  def sendingDataToKafkaTopic(
      topic: String,
      dataToBePassed: Map[String, JsValue],
      kafkaProducer: KafkaProducer[String, String]
  ): Int = {
    try {
      logger.info("sending data to kafka topic")
      dataToBePassed.keysIterator.foreach { key =>
        val record = new ProducerRecord[String, String](
          topic,
          key,
          dataToBePassed(key).toString
        )
        kafkaProducer.send(record)
      }
      kafkaProducer.close()
      1
    } catch {
      case ex: Exception =>
        logger.error(ex.printStackTrace())
        throw new Exception("Unable to send records to topic from Producers")
    }
  }
}
