package com.zyh.flink.TableApi

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{DataTypes, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors._

object Test2 {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)

//    //TODO old planner
//    val settings: EnvironmentSettings = EnvironmentSettings.newInstance().inStreamingMode().useOldPlanner().build()
//    val streamTableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment, settings)
//
//    val batchEnv: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
//    val batchTableEnvironment: BatchTableEnvironment = BatchTableEnvironment.create(batchEnv)

    tableEnvironment.connect( new FileSystem().path("E:\\IdeaProjects\\FlinkDemo\\input\\sensor"))
      .withFormat(new OldCsv)
      .withSchema(new Schema()
      .field("id",DataTypes.STRING())
      .field("time",DataTypes.INT())
      .field("temp",DataTypes.INT()))
      .createTemporaryTable("sensorTable")

    val table: Table = tableEnvironment.from("sensorTable")
    table.toAppendStream[(String,Int,Int)].print()

    environment.execute()
  }

}
