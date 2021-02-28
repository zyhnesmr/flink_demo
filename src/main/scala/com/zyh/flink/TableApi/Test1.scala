package com.zyh.flink.TableApi

import com.zyh.flink.TransformApiTest.SensorDom
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

object Test1 {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val ds: DataStream[String] = environment.readTextFile("input/sensor")
    val objDs: DataStream[SensorDom] = ds.map(s => {
      val strings: Array[String] = s.split(",")
      SensorDom(strings(0), strings(1).toInt, strings(2).toInt)
    })

    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)
    val table: Table = tableEnvironment.fromDataStream(objDs)
    val table1: Table = table.select("id")
    table1.toAppendStream[String].print()
    environment.execute("a")


  }

}
