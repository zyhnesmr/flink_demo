package com.zyh.flink.SinkTest

import java.sql.{Connection, DriverManager, PreparedStatement}

import com.zyh.flink.TransformApiTest.SensorDom
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala._

object MySQLSink {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputDs: DataStream[String] = environment.readTextFile("input/sensor")
    val dsd: DataStream[SensorDom] = inputDs.map(s => {
      val strings: Array[String] = s.split(",")
      SensorDom(strings(0), strings(1).toInt, strings(2).toInt)
    })

    dsd.addSink(new MyJDBCToMYSQLFunction)
    environment.execute("mysql")

  }

}

class MyJDBCToMYSQLFunction extends RichSinkFunction[SensorDom]{
  var connection:Connection = _
  var insertStmt:PreparedStatement = _
  var updateStmt:PreparedStatement = _
  override def open(parameters: Configuration): Unit = {
    connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/testdb?serverTimezone=Asia/Shanghai","root","zyhnesmr")
    insertStmt = connection.prepareStatement("insert into sensor values(?,?)")
    updateStmt = connection.prepareStatement("update sensor set temp = ? where id = ? ")
  }

  override def invoke(value: SensorDom): Unit = {
    updateStmt.setInt(1,value.temporture)
    updateStmt.setString(2,value.id)
    updateStmt.execute()
    if (updateStmt.getUpdateCount()==0){
      insertStmt.setString(1,value.id)
      insertStmt.setInt(2,value.temporture);
      insertStmt.execute()
    }
  }

  override def close(): Unit = {
    insertStmt.close()
    updateStmt.close()
    connection.close()
  }
}