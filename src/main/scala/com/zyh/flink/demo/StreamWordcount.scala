package com.zyh.flink.demo

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala._

object StreamWordcount {

  def main(args: Array[String]): Unit = {
    val tool = ParameterTool.fromArgs(args)
    val host = tool.get("host")
    val port:Int = tool.getInt("port")
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream:DataStream[String] = environment.socketTextStream(host, port)
    val ds = dataStream.flatMap(_.split(" "))
      .map((_, 1)).
      keyBy(0).
      sum(1)
    ds.print()
    //启动任务执行，流处理环境
    environment.execute("wordcount")

  }

}
