package org.why.study.dataset

import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

/**
 * Created by wuheyi on 2019/1/15.
 */
object BulkIterations {
  def main(args: Array[String]): Unit = {
    val env = ExecutionEnvironment.getExecutionEnvironment
    val initial = env.fromElements(1, 2, 3, 4, 5)

    // 将initial按照自定义的函数迭代10次
    val output = initial.iterate(10) {
      iterationInput: DataSet[Int] => {
        iterationInput.map(f => if (f < 3) f + 1 else f - 1)
      }
    }
    output.print()

    // 将initial按照自定义的函数进行迭代，迭代的终止条件为10次或者termination为空
    val output2 = initial.iterateWithTermination(10) {
      iterationInput: DataSet[Int] => {
        val newInput = iterationInput.map(f => if (f < 3) f + 1 else f - 1)
        val termination = newInput.filter(_ == 1)
        (newInput, termination)
      }
    }
    output2.print()


    val initialWorkSet = env.fromElements(0, 1, 2, 3, 4)
    val initialSolutionSet = env.fromElements(1,2,3,4,5)





  }

}
