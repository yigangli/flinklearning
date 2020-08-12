package com.yg.day01

import akka.stream.impl.fusing.Collect
import org.apache.flink.api.common.functions.FlatMapFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object FlatMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    //利用flatmap将id抽取出来，实现map操作
    stream.flatMap(new FlatMapFunction[SensorReading,String] {
      override def flatMap(t: SensorReading, collector: Collector[String]): Unit = {
        collector.collect(t.id)
      }
    }).print()
    //利用flatmap将id为sensor_1的抽取出来，实现filter操作
    stream.flatMap(new FlatMapFunction[SensorReading,SensorReading] {
      override def flatMap(t: SensorReading, collector: Collector[SensorReading]): Unit = {
        if(t.id.equals("sensor_1")){
          collector.collect(t)
        }
      }
    }).print()
    env.execute()
  }
}
