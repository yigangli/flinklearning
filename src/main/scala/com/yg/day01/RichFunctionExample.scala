package com.yg.day01

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._

object RichFunctionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val stream = env.fromElements("hello world")
    stream.map(new MyRichMapFunction).setParallelism(1)
        .print().setParallelism(4)
    env.execute()
  }
  class MyRichMapFunction extends RichMapFunction[String,String]{
    override def open(parameters: Configuration): Unit = {
      print("生命周期开始了")
    }
    override def map(in: String): String = {
      val name = getRuntimeContext.getTaskName
      "任务的名字是:"+name
    }

    override def close(): Unit = print("生命周期结束了")
  }
}
