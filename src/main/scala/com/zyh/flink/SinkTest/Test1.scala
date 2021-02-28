package com.zyh.flink.SinkTest

import com.zyh.flink.TransformApiTest.SensorDom
import org.apache.flink.api.common.serialization.{Encoder, SimpleStringEncoder}
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.scala._


object Test1 {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
        environment.setParallelism(1)
    val ds: DataStream[String] = environment.readTextFile("input/sensor")
    val dsd: DataStream[SensorDom] = ds.map(s => {
      val strings: Array[String] = s.split(",")
      SensorDom(strings(0), strings(1).toInt, strings(2).toInt)
    })

    dsd.print()

    //TODO 测试sink，即结果保存到sink
    //TODO fileSink
    dsd.writeAsCsv("output/res.txt")
    dsd.addSink(StreamingFileSink.forRowFormat(new Path("output/sink"),new SimpleStringEncoder[SensorDom]()).build())
    environment.execute("test")

  }

}
