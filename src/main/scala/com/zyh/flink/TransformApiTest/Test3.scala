package com.zyh.flink.TransformApiTest
import org.apache.flink.streaming.api.scala._

object Test3 {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val ds: DataStream[String] = environment.readTextFile("input/sensor")
    val splitDs: SplitStream[SensorDom] = ds.map(fun = strs => {
      val lines: Array[String] = strs.split(",")
      SensorDom(lines(0), lines(1).toInt, lines(2).toInt)
    }).split(obj => {
      if (obj.temporture > 35) List("high")
      else List("low")
    })

    val lowDs: DataStream[SensorDom] = splitDs.select("low")
    val highDs: DataStream[SensorDom] = splitDs.select("high")
    val addDs: DataStream[SensorDom] = splitDs.select("low", "high")
    lowDs.print("low")
    highDs.print("high")
    addDs.print("all")
    environment.execute("split stream test")
  }
}
