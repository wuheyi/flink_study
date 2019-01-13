package org.why.study.test

import grizzled.slf4j.Logging
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.datastream.DataStreamUtils
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.util.Collector
import org.scalatest.FunSuite

import scala.collection.JavaConverters.asScalaIteratorConverter


/**
 * Created by wuheyi on 2019/1/13.
 */
class FlinkStudySuite extends FunSuite with Logging {
  test("通过DataStreamUtils测试流程序的结果") {
    val env = StreamExecutionEnvironment.createLocalEnvironment()
    val testData = Seq("aa", "bb", "cc")
    val input = env.fromCollection(testData)
    val output = DataStreamUtils.collect(input.javaStream).asScala.toSeq
    assume(testData sameElements output)
  }

  test("批程序的测试") {
    // 使用case class
    val env = ExecutionEnvironment.createLocalEnvironment()
    // 通过fromElements构造测试数据
    val input = env.fromElements(
      WordCount("hello", 1),
      WordCount("dxy", 2),
      WordCount("dxy", 2),
      WordCount("hello", 2),
      WordCount("dxy", 2))
    // groupBy的简单用法
    val output = input.groupBy("word").reduce((w1, w2) => WordCount(w1.word, w1.count + w2.count))
    // flink返回的结果不一定是有序的
    assume(output.collect().sortBy(_.count) equals Seq(WordCount("hello", 3),WordCount("dxy", 6)).sortBy(_.count))
  }

  test("scala 扩展") {
    import org.apache.flink.api.scala.extensions._
    val env = ExecutionEnvironment.createLocalEnvironment()
    val input = env.fromElements(
      WordCount("hello", 1),
      WordCount("dxy", 2),
      WordCount("wuheyi", 100),
      WordCount("hello", 2),
      WordCount("dxy", 2))
    // 支持scala的偏函数，提高代码的可读性
    val output = input.filterWith {
      case WordCount(_, count) => count < 10
    }.groupingBy {
      case WordCount(word, _) => word
    }.reduceWith {
      case (WordCount(word1, count1), WordCount(_, count2)) => WordCount(word1, count1 + count2)
    }.mapWith {
      case WordCount(word, count) => WordCount("new_" + word, count)
    }
    assume(output.collect().sortBy(_.count) equals
      Seq(WordCount("new_hello", 3),WordCount("new_dxy", 4)).sortBy(_.count))
  }

  test("reduceGroup 去重") {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val input = env.fromElements(
      WordCount("hello", 1),
      WordCount("dxy", 2),
      WordCount("wuheyi", 100),
      WordCount("hello", 2),
      WordCount("dxy", 2))
    val output = input.groupBy(_.word).reduceGroup {
      (in, out: Collector[WordCount]) => {
        in.toSet[WordCount].foreach(out.collect(_))
      }
    }
    assume(output.collect().sortBy(_.word) equals
      Seq(WordCount("dxy", 2), WordCount("hello", 1), WordCount("hello", 2), WordCount("wuheyi", 100)).sortBy(_.word))
  }

}
case class WordCount(word: String, count: Int)