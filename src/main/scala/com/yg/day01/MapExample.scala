package com.yg.day01

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.scala._

object MapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream:DataStream[SensorReading] = env.addSource(new SensorSource)

    val mapped1 = stream.map(t=>t.id)
    val mapped2 = stream.map(new MyMapFunction)
    val mapped3 = stream.map(new MapFunction[SensorReading,String]{
      override def map(t: SensorReading): String = t.id
    })

    mapped1.print
    mapped2.print
    mapped3.print
    env.execute()
  }
  //输入泛型SensorReading ,输出泛型String
  class MyMapFunction extends MapFunction[SensorReading,String]{
    override def map(t: SensorReading): String = t.id
  }

}
