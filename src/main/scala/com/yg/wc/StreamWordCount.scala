package com.yg.wc

import org.apache.flink.streaming.api.scala._

object StreamWordCount {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val dataStream = env.socketTextStream("localhost",5678)
    val wordCount = dataStream.flatMap(_.split("\\s+"))
      .map((_,1))
      .keyBy(0).sum(1)
      wordCount.print()
    env.execute()

  }
}
