package com.yg.day01

import org.apache.flink.api.common.functions.FilterFunction
import org.apache.flink.streaming.api.scala._

object FilterExample{
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    val filter1 = stream.filter(t=>{
      t.id.endsWith("_1")
    })
    val filter2 = stream.filter(new MyFilter)
    val filter3 = stream.filter(new FilterFunction[SensorReading] {
      override def filter(t: SensorReading): Boolean = {
        t.id.hashCode%2==1
      }
    })
//    filter1.print
//    filter2.print
    filter3.print
    env.execute()
  }
  class MyFilter extends FilterFunction[SensorReading]{
    override def filter(t: SensorReading): Boolean = {
      t.id.hashCode%3==1
    }
  }
}
