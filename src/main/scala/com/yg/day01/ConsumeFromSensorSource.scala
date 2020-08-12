package com.yg.day01

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment

object ConsumeFromSensorSource {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    stream.print()
    env.execute()
  }
}
