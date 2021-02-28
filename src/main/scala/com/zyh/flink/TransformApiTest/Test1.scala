package com.zyh.flink.TransformApiTest

import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.scala._

object Test1 {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
//    environment.setParallelism(1)
    val ds: DataStream[String] = environment.readTextFile("input/sensor")
    ds.map(s=>{
      val strings: Array[String] = s.split(",")
      SensorDom(strings(0),strings(1).toInt,strings(2).toInt)
    }).keyBy(0).min(2).print()

    environment.execute("test")

  }

}

case class SensorDom(id:String,code:Int,temporture:Int)
