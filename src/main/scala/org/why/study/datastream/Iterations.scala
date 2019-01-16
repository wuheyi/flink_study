package org.why.study.datastream

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.createTuple2TypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

/**
 * Created by wuheyi on 2019/1/16.
 */
object Iterations {
  def main(args: Array[String]): Unit = {
    // Checking input parameters
    val params = ParameterTool.fromArgs(args)
    // obtain execution environment and set setBufferTimeout to 1 to enable
    // continuous flushing of the output buffers (lowest latency)
    val env = StreamExecutionEnvironment.getExecutionEnvironment.setBufferTimeout(1)
    // make parameters available in the web interface
    env.getConfig.setGlobalJobParameters(params)

    def iterationStep(iteration: DataStream[Long]) = {
        val minusOne = iteration.map( v => v - 1)
        val stillGreaterThanZero = minusOne.filter (_ > 0)
        val lessThanZero = minusOne.filter(_ <= 0)
        (stillGreaterThanZero, lessThanZero)
    }

    val someIntegers = env.generateSequence(0, 10)
    val iteratedStream = someIntegers.iterate(iterationStep)
    iteratedStream.print()

    // execute the program
    env.execute("Streaming Iteration Example")

  }

}
