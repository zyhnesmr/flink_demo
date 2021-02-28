package com.zyh.flink.SinkTest

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}

object Test2 {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val properties: Properties = new Properties()
    properties.put("bootstrap.servers", "node1:9092")
    properties.put("group.id", "flink")
    properties.put("enable.auto.commit", "true")
    properties.put("auto.commit.interval.ms", "1000")
    val strDs: DataStream[String] = environment.addSource(new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties))
    val resDs: DataStreamSink[String] = strDs.addSink(new FlinkKafkaProducer011[String]("node1:9092", "test", new SimpleStringSchema))
    environment.execute("kafka")
  }

}
