package com.zyh.flink.TransformApiTest

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.streaming.api.scala._

object Test2 {

  def main(args: Array[String]): Unit = {

      val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
      //    environment.setParallelism(1)
      val ds: DataStream[String] = environment.readTextFile("input/sensor")
    //TODO 可以这么写
//      ds.map(s=>{
//        val strings: Array[String] = s.split(",")
//        SensorDom(strings(0),strings(1).toInt,strings(2).toInt)
//      }).keyBy(0)
//          .reduce((s1,s2)=>{
//            SensorDom(s1.id,math.max(s1.code,s2.code),math.min(s1.temporture,s2.temporture))
//          })
//          .print()
    //TODO 也可以用Java的类，ReduceFunction
    ds.map(s=>{
              val strings: Array[String] = s.split(",")
              SensorDom(strings(0),strings(1).toInt,strings(2).toInt)
            })
        .keyBy("id")
        .reduce(new MyReduceFunction)
        .print()


      environment.execute("test")

    }

}

class MyReduceFunction extends ReduceFunction[SensorDom]{
  override def reduce(value1: SensorDom, value2: SensorDom): SensorDom = {
    SensorDom(value1.id,math.max(value1.code,value2.code),math.min(value1.temporture,value2.temporture))
  }
}
