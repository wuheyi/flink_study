package org.why.study.datastream.socket

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.joda.time.DateTime
import org.why.study.trigger.MyEventTimeTrigger

/**
 * Created by wuheyi on 2019/5/24.
 */
object SocketStudy {

  case class SocketData(atime: Long, data: String, cnt: Int)

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val text: DataStream[String] = env.socketTextStream("localhost", 9999, '\n')

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

    val socketStream = text.map(w => {
      val raw = w.split(",")
      val atime = raw(0)
      val data = raw(1)
      val cnt = raw(2).toInt
      SocketData(new DateTime(atime).getMillis, data, cnt)
    })

    val withTimestampsAndWatermarks = socketStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[SocketData](Time.seconds(1)){
        override def extractTimestamp(t: SocketData): Long = t.atime
      })

    val result = withTimestampsAndWatermarks
      .keyBy(_.data)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .trigger(MyEventTimeTrigger.create())
      .sum("cnt")
      .map(f => new DateTime(f.atime).toString("MM/dd/yyyy HH:mm:ss.SSS"))

    result.print()

    env.execute()




  }

}
