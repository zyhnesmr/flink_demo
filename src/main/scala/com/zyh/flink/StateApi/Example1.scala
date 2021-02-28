package com.zyh.flink.StateApi

import com.zyh.flink.TransformApiTest.SensorDom
import org.apache.flink.api.common.functions.RichFlatMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Example1 {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val ds: DataStream[String] = environment.readTextFile("input/sensor")
    val objDs: DataStream[SensorDom] = ds.map(s => {
      val strings: Array[String] = s.split(",")
      SensorDom(strings(0), strings(1).toInt, strings(2).toInt)
    })
    //TODO 两次温度超过十度，输出抱紧信息
    val resDs = objDs.keyBy(_.id)
//      .flatMap(new TempChangeAlert(10))
        .flatMapWithState[(String,Int,Int),Int]{
          case (sensorDom:SensorDom,None)=>(List.empty,Some(sensorDom.temporture))
          case (sensorDom:SensorDom,state:Some[Int])=>{
            val abs = (sensorDom.temporture - state.get).abs
            if (abs>10){
              (List((sensorDom.id,state.get,sensorDom.temporture)),Some(sensorDom.temporture))
            }else{
              (List.empty,Some(sensorDom.temporture))
            }
        }
      }

    resDs.print()
    environment.execute()


  }

}

class TempChangeAlert(famen:Int) extends RichFlatMapFunction[SensorDom,(String,Int,Int)]{
  lazy val valueState: ValueState[Int] = getRuntimeContext.getState(new ValueStateDescriptor[Int]("preTemp", classOf[Int]))
  override def flatMap(value: SensorDom, out: Collector[(String,Int,Int)]): Unit = {
    val preTemp = valueState.value()
    val chazhi = (value.temporture - preTemp).abs
    if (chazhi>famen){
      out.collect((value.id,preTemp,value.temporture))
    }
    valueState.update(value.temporture)
  }
}
