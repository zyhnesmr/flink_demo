package com.zyh.flink.ProcessFunction

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object SideOutputTest {


  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDs: DataStream[String] = environment.socketTextStream("localhost", 9999)
    val transDs: DataStream[String] = inputDs.process(new MyPC)
    transDs.print()
    transDs.getSideOutput(OutputTag[(String,Int)]("side")).print()
    environment.execute()

  }
}

class MyPC extends ProcessFunction[String,String]{
  override def processElement(value: String, ctx: ProcessFunction[String, String]#Context, out: Collector[String]): Unit = {
    out.collect("main:"+value)
    if (value.contains("a")){
      ctx.output( OutputTag[(String,Int)]("side"),("side A:"+value,5))
    }
  }
}
