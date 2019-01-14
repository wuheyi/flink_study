package org.why.study

import org.apache.flink.api.common.operators.base.JoinOperatorBase.JoinHint
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

/**
 * Created by wuheyi on 2019/1/14.
 */
object DataSetJoinApi {
  case class WordInfo(word: String, count: Int, category: String)
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.createLocalEnvironment()
    val input1 = env.fromCollection(Seq(("aaa", 1), ("bbb", 2)))
    val input2 = env.fromCollection(Seq(WordInfo("aaa", 3, "small"),
      WordInfo("bbb", 10, "big"), WordInfo("ccc", 3, "small")))

    // JOIN 的 一般用法形式
    input1.join(input2).where(_._1).equalTo(_.word).print()
    input1.join(input2).where(0).equalTo("word").print()

    // 可以通过自定义函数,对join后的结果做转化
    input1.join(input2).where(0).equalTo("word") {
      (in1, in2) => (in2.word, in2.category, in1._2)
    }.print()

    // 通过自定义函数，对结果做扁平化
    input1.join(input2).where(0).equalTo("word") {
      (in1, in2, out: Collector[(String, Int)]) =>
        if(in2.category == "big") out.collect((in2.word, in1._2))
    }.print()

    // 暗示哪张表是大的或者小的，提升性能
    input1.joinWithTiny(input2).where(0).equalTo(_.word).print()
    input1.joinWithHuge(input2).where(0).equalTo(_.word).print()

    // 默认情况，系统自动选择优化方案
    input1.join(input2, JoinHint.OPTIMIZER_CHOOSES).where(0).equalTo(_.word)
    // 适用于第一个表非常小
    input1.join(input2, JoinHint.BROADCAST_HASH_FIRST).where(0).equalTo(_.word)
    // 适用于第二个表非常小
    input1.join(input2, JoinHint.BROADCAST_HASH_SECOND).where(0).equalTo(_.word)
    // 适用于第一个表比第二个表小，但两个表都比较大
    input1.join(input2, JoinHint.REPARTITION_HASH_FIRST).where(0).equalTo(_.word)
    // 适用于第二个表比第一个表小，但两个表都比较大
    input1.join(input2, JoinHint.REPARTITION_HASH_SECOND).where(0).equalTo(_.word)
    // 适用于任意一个表已经排序
    input1.join(input2, JoinHint.REPARTITION_SORT_MERGE).where(0).equalTo(_.word)

    // 外连接的含义和sql的含义一致
    input1.leftOuterJoin(input2).where(0).equalTo(_.word)
    input1.rightOuterJoin(input2).where(0).equalTo(_.word)
    input1.fullOuterJoin(input2).where(0).equalTo(_.word)

    /**
     * NOTE: Not all execution strategies are supported by every outer join type, yet.
    LeftOuterJoin supports:
      OPTIMIZER_CHOOSES
      BROADCAST_HASH_SECOND
      REPARTITION_HASH_SECOND
      REPARTITION_SORT_MERGE
    RightOuterJoin supports:
      OPTIMIZER_CHOOSES
      BROADCAST_HASH_FIRST
      REPARTITION_HASH_FIRST
      REPARTITION_SORT_MERGE
    FullOuterJoin supports:
      OPTIMIZER_CHOOSES
      REPARTITION_SORT_MERGE
     */








  }

}
