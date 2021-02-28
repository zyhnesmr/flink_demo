package com.zyh.refresh.test

import scala.util.Random

object Test {

  def main(args: Array[String]): Unit = {
    while (true){
      println(Random.nextInt(3))
      Thread.sleep(1000)
    }
  }

}
