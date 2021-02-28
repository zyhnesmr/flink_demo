package com.zyh.flink.WindowsApi

import com.zyh.flink.TransformApiTest.SensorDom
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

object Test1 {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val socketDs: DataStream[String] = environment.socketTextStream("localhost", 9999)
    val dataDs: DataStream[SensorDom] = socketDs.map(s => {
      val strings: Array[String] = s.split(",")
      SensorDom(strings(0), strings(1).toInt, strings(2).toInt)
    })
    val resDs: DataStream[(String, Int)] = dataDs.map(d => {
      (d.id, d.temporture)
    }).keyBy(_._1)
      .timeWindow(Time.seconds(15))
      .reduce((t1, t2) => {
        (t1._1, t1._2 + t2._2)
      })
    resDs.print()
    environment.execute("hello")
  }

}
