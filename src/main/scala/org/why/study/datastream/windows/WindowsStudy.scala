package org.why.study.datastream.windows

import org.apache.flink.api.common.functions.{AggregateFunction, RichFoldFunction}
import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.scala.function.util.ScalaFoldFunction
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector


object WindowsStudy {

  case class LoginEvent(userId: String, ip: String, `type`: String, timestamp: Long, cnt: Int)

  case class LoginWarning(userId: String, `type`: String, ip: String)

  case class LoginAverage(userId: String, windowEnd: Long, average: Double)

  case class LoginCount(userId: String, windowEnd: Long, count: Double)

  case class LoginAccumulator(sum: Int, cnt: Int)

  class AverageAggregate extends AggregateFunction[LoginEvent, LoginAccumulator, Double] {
    override def createAccumulator() = LoginAccumulator(0, 0)

    override def add(value: LoginEvent, accumulator: LoginAccumulator): LoginAccumulator = {
      LoginAccumulator(accumulator.sum + value.cnt, accumulator.cnt + 1)
    }

    override def getResult(accumulator: LoginAccumulator): Double = {
      accumulator.sum / accumulator.cnt
    }

    override def merge(a: LoginAccumulator, b: LoginAccumulator): LoginAccumulator = {
      LoginAccumulator(a.sum + b.sum, a.cnt + b.cnt)
    }

  }

  class AverageProcessWindowFunction extends ProcessWindowFunction[Double, LoginAverage, String, TimeWindow] {
    @scala.throws[Exception]
    override def process(key: String, context: Context, elements: Iterable[Double], out: Collector[LoginAverage]): Unit = {
      val average = elements.iterator.next()
      out.collect(LoginAverage(key, context.window.getEnd(), average))
    }
  }

  class CountProcessWindowFunction extends ProcessWindowFunction[LoginEvent, LoginCount, String, TimeWindow] {
    @scala.throws[Exception]
    override def process(key: String, context: Context, elements: Iterable[LoginEvent], out: Collector[LoginCount]): Unit = {
        val average = elements.iterator.next()
        out.collect(LoginCount(key, context.window.getEnd(), average.cnt))
    }
  }


  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);


    val loginEventStream = env.fromCollection(List(
      LoginEvent("1", "192.168.0.1", "fail", 1000L, 1),
      LoginEvent("1", "192.168.0.2", "fail", 1800L, 2),
      LoginEvent("1", "192.168.0.3", "fail", 2500L, 3),
      LoginEvent("2", "192.168.10,10", "fail", 3200L, 4)
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

    // reduce 方法的输入和输出的数据类型需要保持一致，比较局限
//    val result = withTimestampsAndWatermarks
//      .keyBy(_.userId)
//      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//      .reduce((v1, v2) => LoginEvent(v1.userId, v1.ip, v1.`type`, v1.timestamp, v1.cnt + v2.cnt))

//    val result = withTimestampsAndWatermarks
//      .keyBy(_.userId)
//      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//      .aggregate(new AverageAggregate())

//    val result = withTimestampsAndWatermarks
//      .keyBy(_.userId)
//      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//      .fold(LoginEvent("0", "0.0.0.0", "default", 0L, 0)) {
//        (acc, v) => LoginEvent(v.userId, v.ip, v.`type`, v.timestamp, v.cnt + acc.cnt)
//      }

//    val result = withTimestampsAndWatermarks
//      .keyBy(_.userId)
//      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
//      .aggregate(new AverageAggregate(), new AverageProcessWindowFunction())

    val result = withTimestampsAndWatermarks
      .keyBy(_.userId)
      .window(TumblingEventTimeWindows.of(Time.seconds(3)))
      .fold(LoginEvent("0", "0.0.0.0", "default", 0L, 0),
        (acc: LoginEvent, v: LoginEvent) => LoginEvent(v.userId, v.ip, v.`type`, v.timestamp, v.cnt + acc.cnt),
        new CountProcessWindowFunction()
      )


    result.print()

    env.execute()

  }

}

