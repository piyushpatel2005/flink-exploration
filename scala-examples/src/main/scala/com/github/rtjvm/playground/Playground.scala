package com.github.rtjvm.playground

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment


object Playground {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val data = env.fromElements(1, 2, 3, 4, 5)
    data.print()
    env.execute("My first Flink Job")
  }
}
