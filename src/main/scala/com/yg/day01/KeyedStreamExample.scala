package com.yg.day01

import org.apache.flink.streaming.api.scala._

object KeyedStreamExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream = env.addSource(new SensorSource)
    //第二个泛型是key的类型
    val keyed:KeyedStream[SensorReading,String]=stream.keyBy(_.id)
    val mintemporature = keyed.min(2)//使用第三个字段坐滚动聚合,求流上的最小温度值
    //mintemporature.filter(_.id=="sensor_1").print()
    val mintemporature2 = keyed.reduce((k1,k2)=>new SensorReading(k1.id,0L,k1.temperature.min(k2.temperature)))
    mintemporature2.filter(_.id=="sensor_1").print()
    env.execute()
  }
}
