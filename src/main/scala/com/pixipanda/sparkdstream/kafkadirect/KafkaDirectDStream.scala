package com.pixipanda.sparkdstream.kafkadirect

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka010.ConsumerStrategies._
import org.apache.spark.streaming.kafka010.KafkaUtils
import org.apache.spark.streaming.kafka010.LocationStrategies._
import org.apache.spark.streaming.{Seconds, StreamingContext}


object  KafkaDirectDStream {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val topic = args(1)

    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Join Stream With Static Data")
      .getOrCreate()



    val kafkaParams = Map[String, String](
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "localhost:9092",
      ConsumerConfig.GROUP_ID_CONFIG -> "SparkKafkaDirectStreamGroup",
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG ->
        "org.apache.kafka.common.serialization.StringDeserializer",
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest"

    )

    val ssc = new StreamingContext(sparkSession.sparkContext, Seconds(5))

    val topicSet = Set(topic)

    val kafkaDStreams = KafkaUtils.createDirectStream[String, String](ssc,
                       PreferConsistent,
                       Subscribe[String, String](topicSet, kafkaParams)
                       ).map(cr => cr.value)


    kafkaDStreams.print()

    ssc.start()
    ssc.awaitTermination()
  }
}