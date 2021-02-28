package com.zyh.flink.ApiTestSource

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala._

object SourceTest {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val strings: List[String] = List("asa", "sas", "sda", "hello")
    val ds: DataStream[String] = environment.fromCollection(strings)
    ds.map((_,1)).keyBy(0).sum(1).print()
    environment.execute()
  }

}
