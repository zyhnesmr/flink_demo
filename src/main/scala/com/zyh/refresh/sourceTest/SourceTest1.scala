package com.zyh.refresh.sourceTest

import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala._

import scala.util.Random

object SourceTest1{

  def main(args: Array[String]): Unit = {


    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDs: DataStream[String] = environment.addSource(new NewSourceFunc)

    inputDs.flatMap(str=>str.split(","))
      .map(str=>(str,1))
      .keyBy(_._1)
      .reduce((a,b)=>(a._1,a._2+b._2))
      .print()

    environment.execute()
  }

}

class NewSourceFunc extends SourceFunction[String]{
  var endTag = false
  val stringArray = Array[String]("coco","bill","lesson","cuiwenhui")
  override def run(ctx: SourceFunction.SourceContext[String]): Unit = {
    while (!endTag){
      var ans = ""
      for (i<- 0 to 1){
        ans+=stringArray(Random.nextInt(4))+","
      }
      ans+=stringArray(Random.nextInt(4))
      println(ans)
      ctx.collect(ans)
      Thread.sleep(3000)
    }
  }

  override def cancel(): Unit = endTag = true
}
