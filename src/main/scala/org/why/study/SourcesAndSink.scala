package org.why.study

import java.io.File

import org.apache.flink.api.scala.ExecutionEnvironment
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.fs.FileSystem.WriteMode
import org.apache.flink.streaming.api.scala._

/**
 * Created by wuheyi on 2019/1/15.
 */
object SourcesAndSink {
  case class Person(name: String, age: Int)
  def main(args: Array[String]): Unit = {
    val env  = ExecutionEnvironment.getExecutionEnvironment
    // read text file from local files system
    // 也可以读取hdfs文件
    val basedir = "file://" + System.getProperty("user.dir") + File.separator + "files" + File.separator
    val textdir = basedir + "textfile"
    env.readTextFile(textdir).print()
    // read a CSV file with three fields
    // 读取的内容可以直接转换成case class，可以指定读取的列，可以设置是否忽略第一行，其他配置参考官网
    val csvdir = basedir + "csvfile.csv"
    env.readCsvFile[(String, Int)](csvdir, includedFields = Array(0, 1), ignoreFirstLine = true).print()
    val input = env.readCsvFile[Person](csvdir, includedFields = Array(0, 1), ignoreFirstLine = true)
    import org.apache.flink.api.common.operators.Order
    val parameters = new Configuration()
    parameters.setBoolean("recursive.file.enumeration", true)
    input.print()

    // sortPartition 本地排序，用于测试还不错
    input.sortPartition(_.name, Order.ASCENDING).writeAsCsv(basedir + "newcsv.csv", "\n", "|", WriteMode.OVERWRITE)
    env.readCsvFile[Person](basedir + "newcsv.csv").print()

    // 递归读取目录
    val nestsdir = basedir + "nests"
    env.readTextFile(nestsdir).withParameters(parameters).print()

    // 支持压缩文件的读取
    val gzdir = basedir + "textfile2.gz"
    env.readTextFile(gzdir).print()

  }

}
