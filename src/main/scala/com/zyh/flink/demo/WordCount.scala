package com.zyh.flink.demo

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.api.scala._

object WordCount {

  def main(args: Array[String]): Unit = {

    val environment: ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val path = "input/hello.txt"
    val ds:DataSet[String] = environment.readTextFile(path)
//    ds.flatMap(fun = (s, collecter:Collector[(String,Int)]) => {
//      val strings = s.split(" ")
//      for (word<-strings){
//        collecter.collect((word,1))
//      }
//    })


    val dataset:GroupedDataSet[(String, Int)]= ds.flatMap(_.split(" ")).map((_, 1)).groupBy(0)
    val value = dataset.sum(1)
    value.print()



  }

}
