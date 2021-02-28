package com.zyh.flink.ProcessFunction

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object Test1 {


  def main(args: Array[String]): Unit = {
    val environment = StreamExecutionEnvironment.getExecutionEnvironment
    val socketds = environment.socketTextStream("localhost", 9999)
    val dataStream = socketds.map(s => {
      val strings = s.split(",")
      SensorBean(strings(0), strings(1).toDouble, strings(2).toDouble)
    })
    dataStream.print()

    val keyedDs = dataStream.keyBy(_.id)
    val resDs = keyedDs.process(new MyKeyedProcessFucntion(6000L))
    resDs.print()
    environment.execute()
  }
}

class MyKeyedProcessFucntion (interval:Long) extends KeyedProcessFunction[String,SensorBean,String]{
  lazy val lastTemp: ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("lastTemp", classOf[Double], 0))
  lazy val lastTimer: ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("lastTimer", classOf[Long], 0))
  override def processElement(value: SensorBean, ctx: KeyedProcessFunction[String, SensorBean, String]#Context, out: Collector[String]): Unit = {
    if (value.temp>lastTemp.value()&&lastTimer.value()==0){
      val ts = ctx.timerService().currentProcessingTime()+interval;
      ctx.timerService().registerProcessingTimeTimer(ts)
      lastTimer.update(ts);
    }else if (value.temp<lastTemp.value()){
      ctx.timerService().deleteProcessingTimeTimer(lastTimer.value())
      lastTimer.update(0);
    }
    lastTemp.update(value.temp)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorBean, String]#OnTimerContext, out: Collector[String]): Unit = {
    out.collect("temp out of limition")
    lastTimer.update(0)
  }
}

case class SensorBean(id:String,time:Double,temp:Double)
