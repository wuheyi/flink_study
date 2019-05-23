package org.why.study.datastream


import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks
import org.apache.flink.streaming.api.functions.timestamps.{AscendingTimestampExtractor, BoundedOutOfOrdernessTimestampExtractor}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.watermark.Watermark
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time

import scala.collection.Map

object FlinkLoginFail {

  case class LoginEvent(userId: String, ip: String, `type`: String, timestamp: Long, cnt: Int)

  case class LoginWarning(userId: String, `type`: String, ip: String)


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


    val loginEventStream = env.fromCollection(List(
      new LoginEvent("1", "192.168.0.1", "fail", 100L, 1),
      new LoginEvent("1", "192.168.0.2", "fail", 1800L, 2),
      new LoginEvent("1", "192.168.0.3", "fail", 3500L, 3),
      new LoginEvent("2", "192.168.10,10", "success", 200L, 4)
    ))

//    loginEventStream.assignTimestampsAndWatermarks(new AscendingTimestampExtractor[LoginEvent] {
//      def extractAscendingTimestamp(element: LoginEvent): Long = element.getTimestamp
//    })

    val withTimestampsAndWatermarks = loginEventStream.assignTimestampsAndWatermarks(
      new BoundedOutOfOrdernessTimestampExtractor[LoginEvent](Time.seconds(1)){
        override def extractTimestamp(t: LoginEvent): Long = t.timestamp
      })


//    val result = withTimestampsAndWatermarks
//      .keyBy(_.userId)
//      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//      .sum("cnt")
//
//    result.print()
//
//    env.execute()

    val loginFailPattern = Pattern.begin[LoginEvent]("begin")
      .where(_.`type`.equals("fail"))
      .next("next")
      .where(_.`type`.equals("fail"))
      .within(Time.seconds(3))

    val patternStream = CEP.pattern(withTimestampsAndWatermarks, loginFailPattern)

    val loginFailDataStream = patternStream
      .select((pattern: Map[String, Iterable[LoginEvent]]) => {
        val first = pattern.getOrElse("begin", null).iterator.next()
        val second = pattern.getOrElse("next", null).iterator.next()

        LoginWarning(second.userId, second.ip, second.`type`)
      })

    loginFailDataStream.print

    env.execute
  }

}

