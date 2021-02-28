package com.zyh.flink.ApiTestSource

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._

object KafkaSource {


  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = new Properties()
    properties.put("bootstrap.servers", "node1:9092")
    properties.put("group.id", "flink")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "1000")
    val value: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    value.print()
    environment.execute("kafkaTest")
  }

}
