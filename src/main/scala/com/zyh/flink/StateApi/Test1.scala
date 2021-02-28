package com.zyh.flink.StateApi

import com.zyh.flink.TransformApiTest.SensorDom
import org.apache.flink.api.common.functions.{ReduceFunction, RichMapFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ReducingState, ReducingStateDescriptor, ValueState, ValueStateDescriptor}
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

  }

}

class MyRichMapper extends RichMapFunction[SensorDom,String]{
  lazy val valueState:ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("temp",classOf[Int]))
  lazy val listState: ListState[Int] = getRuntimeContext.getListState(new ListStateDescriptor[Int]("tempOfList", classOf[Int]))
  lazy val reduceState: ReducingState[Int] = getRuntimeContext.getReducingState(new ReducingStateDescriptor[Int]("reduceTemp", new ReduceFunction[Int] {
    override def reduce(value1: Int, value2: Int): Int = {
      value1+value2
    }
  }, classOf[Int]))
  override def map(value: SensorDom): String = {
    valueState.update(value.temporture);
    listState.add(value.temporture)
    reduceState.add(value.temporture)
    value.id
  }
}
