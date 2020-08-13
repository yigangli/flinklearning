package com.yg.day01

import org.apache.flink.streaming.api.scala._

object UnionExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    val s1 = stream.filter(_.id.equals("sensor_1"))
    val s2 = stream.filter(_.id.equals("sensor_2"))
    val s3 = stream.filter(_.id.equals("sensor_3"))
    val union:DataStream[SensorReading] = s1.union(s2,s3)
    union.print()
    env.execute()
  }
}
