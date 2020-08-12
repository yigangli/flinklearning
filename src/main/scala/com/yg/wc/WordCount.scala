package com.yg.wc

import org.apache.flink.api.scala._

object WordCount {
  def main(args: Array[String]): Unit = {
    val env:ExecutionEnvironment = ExecutionEnvironment.getExecutionEnvironment
    val dataSet = env.readTextFile("src\\main\\resources\\word.txt")
    dataSet.flatMap(_.split("//s+"))
      .map((_,1)).groupBy(0)
      .sum(1)
      .print()
  }
}
