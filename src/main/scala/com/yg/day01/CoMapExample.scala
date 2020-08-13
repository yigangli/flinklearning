package com.yg.day01

import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala._

object CoMapExample {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    val stream1:DataStream[(String,Int)] = env.fromElements(
      ("zuoyuan",130),("baiyuan",120)
    )
    val stream2:DataStream[(String,Int)] = env.fromElements(
      ("zuoyuan",22),("baiyuan",33)
    )
    //method1
    val connectedStream:ConnectedStreams[(String,Int),(String,Int)] = stream1.keyBy(_._1)
      .connect(stream2.keyBy(_._1))
    val res = connectedStream.map(new MyMapFunction)
    //method2
    val conned:ConnectedStreams[(String,Int),(String,Int)] = stream1.connect(stream2)
    val res2 = conned.keyBy(0,0).map(new MyMapFunction)
    res.print()
    res2.print()
    //broadcast
    stream1.connect(stream2.broadcast)

    env.execute()
  }
  class MyMapFunction extends CoMapFunction[(String,Int),(String,Int),String]{
    override def map1(in1: (String, Int)): String = {
      in1._1+"的体重是"+in1._2+"斤"
    }

    override def map2(in2: (String, Int)): String = {
      in2._1+"的年龄是"+in2._2+"岁"
    }
  }
}
