package com.zyh.flink.TableApi

import com.zyh.flink.TransformApiTest.SensorDom
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{Table, Tumble}
import org.apache.flink.table.api.scala._
import org.apache.flink.types.Row

object TimeAndWindowTesty {


  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)

    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)

    val inputds: DataStream[String] = environment.readTextFile("input/sensor")
    val dataStream: DataStream[SensorDom] = inputds.map(s => {
      val strings: Array[String] = s.split(",")
      SensorDom(strings(0), strings(1).toInt, strings(2).toInt)
    })
    val sensorTable: Table = tableEnvironment.fromDataStream(dataStream, 'id, 'code, 'temporture as 'temp, 'pt.proctime as 'time)
//    sensorTable.printSchema()

    val resultTable: Table = sensorTable
      .window(Tumble.over(10.seconds).on('time).as('w))
      .groupBy('id, 'w)
      .select('id, 'id.count, 'temp.avg, 'w.end)

    resultTable.toAppendStream[Row].print()
//    sensorTable.toAppendStream[Row].print()



    environment.execute()

  }
}
