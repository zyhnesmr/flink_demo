package com.zyh.flink.TableApi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{FileSystem, OldCsv, Schema}

object OutputTest {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)

    tableEnvironment.connect( new FileSystem().path("E:\\IdeaProjects\\FlinkDemo\\input\\sensor"))
      .withFormat(new OldCsv)
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("time",DataTypes.INT())
        .field("temp",DataTypes.INT()))
      .createTemporaryTable("sensorTable")

//    val table: Table = tableEnvironment.from("sensorTable")
//    table.groupBy('id).select('id,'id.count).toRetractStream[(String,Long)].print()
    val table: Table = tableEnvironment.from("sensorTable")

    tableEnvironment.connect( new FileSystem().path("E:\\IdeaProjects\\FlinkDemo\\input\\output.txt"))
      .withFormat(new OldCsv)
      .withSchema(new Schema()
        .field("id",DataTypes.STRING())
        .field("temp",DataTypes.INT()))
      .createTemporaryTable("outputTable")

    table.select('id,'temp).insertInto("outputTable")

    environment.execute()
  }

}
