package org.why.study.dataset

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.streaming.api.scala._

/**
 * Created by wuheyi on 2019/1/14.
 */
object PartitionApi {

  case class WordInfo(word: String, count: Int, category: String)

  def main(args: Array[String]): Unit = {

    val env = ExecutionEnvironment.createLocalEnvironment()
    val input = env.fromCollection(Seq(("aaa", 1), ("bbb", 2)))
    println(input.getParallelism)
    /**
     * 暂时还不清楚各个partition分区算子的作用，
     * 可能和spark的分区差不多，
     * 是为了防止数据倾斜，
     * 后续需要回过头来再看，
     * 另外自定义分区函数还需要了解
     */
    input.partitionByHash(0).mapPartition(iterator => iterator.map(_._2)).print()
    input.partitionByRange(0).mapPartition(iterator => iterator.map(_._2)).print()
    input.sortPartition(0, Order.ASCENDING).sortPartition(1, Order.DESCENDING).print()
    println(input.rebalance().map(_._1).getParallelism)


  }

}
