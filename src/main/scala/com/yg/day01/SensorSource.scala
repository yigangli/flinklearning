package com.yg.day01

import java.util.Calendar

import org.apache.flink.streaming.api.functions.source.SourceFunction.SourceContext
import org.apache.flink.streaming.api.functions.source.{RichParallelSourceFunction, SourceFunction}

import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading]{
  var running:Boolean = true

  override def run(ctx: SourceContext[SensorReading]): Unit = {
    val rand = new Random()
    var curFtemp = (1 to 10).map(
      i => ("sensor_"+i,(rand.nextGaussian()*20))
    )
    while(running){
      curFtemp = curFtemp.map(
        t=>(t._1,t._2+(rand.nextGaussian()*0.5))
      )
      val curTime = Calendar.getInstance().getTimeInMillis
      curFtemp.foreach(t=>ctx.collect(SensorReading(t._1,curTime,t._2)))
      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = running = false
}
