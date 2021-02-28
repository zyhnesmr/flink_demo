package com.zyh.flink.ApiTestSource

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object DesignedSource {


  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val value: DataStream[String] = environment.addSource(new MyDesignSourceFunction)
    value.print()
    environment.execute("design source")
  }

}

class MyDesignSourceFunction extends SourceFunction[String]{
  var runningFlag: Boolean = true;
  override def run(sourceContext: SourceFunction.SourceContext[String]): Unit = {
    //TODO 随机数产生器
    val self: java.util.Random = Random.self

    while (runningFlag){
      Thread.sleep(500)
      sourceContext.collect("Sensor_"+self.nextInt()%4+":"+System.currentTimeMillis())
    }
  }

  override def cancel(): Unit = runningFlag = false
}
