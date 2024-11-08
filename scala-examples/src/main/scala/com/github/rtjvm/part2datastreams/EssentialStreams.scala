package com.github.rtjvm.part2datastreams

import org.apache.flink.api.common.functions.{FlatMapFunction, MapFunction, ReduceFunction}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.streaming.api.scala.KeyedStream
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala.createTypeInformation
import org.apache.flink.util.Collector

object EssentialStreams {

  private def applicationTemplate(): Unit = {
    // 1. Need to create a StreamExecutionEnvironment
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.createLocalEnvironment(1)

    // add any sort of computations
    val simpleNumberStream: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    simpleNumberStream.print()

    val stringStream: DataStream[String] = env.fromElements("Hello", "World")
    stringStream.print()

    // At the end
    env.execute("My first Flink Job")
  }

  def demoTransofrmations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val numbers: DataStream[Int] = env.fromElements(1, 2, 3, 4, 5)

    // checking parallellism
    println(s"Current parallelism : ${env.getParallelism}")
    env.setParallelism(2)
    println(s"New parallelism : ${env.getParallelism}")
    val doubleNumbers = numbers.map(_ * 2)
    //    doubleNumbers.print()

    // flatMap
    val expandedNumbers: DataStream[Int] = numbers.flatMap(num => List(num, num + 1))
    val finalData = expandedNumbers.writeAsText("src/main/resources/output/expandedNumbers") // deprecated

    // You can set parallelism on DataStream level using setParallelism
    finalData.setParallelism(1)
    val evenNumbers: DataStream[Int] = numbers.filter(_ % 2 == 0).setParallelism(2)
    //    evenNumbers.print()

    env.execute()
  }

  /**
   * Exercise: Fizzbuzz on Flink
   * - Take a stream of 100 numbers
   * - For each number, if n % 3 == 0 then return "fizz"
   * - If n % 5 == 0 then return "buzz"
   * - If n % 3 == 0 and n % 5 == 0 then return "fizzbuzz"
   * - print the numbers for which you said "fizzbuzz" to a file
   * @param args
   */
  case class FizzBuzzResult(number: Long, result: String)
  def fizzBuzz(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val numbers = env.fromSequence(1, 100)
    val fizzbuzz: DataStream[FizzBuzzResult] = numbers.map({ n =>
      val output: String = if (n % 3 == 0 && n % 5 == 0) "fizzbuzz"
      else if (n % 3 == 0) "fizz"
      else if (n % 5 == 0) "buzz"
      else n.toString
      FizzBuzzResult(n, output)
    })

    val fizzBuzzNumbers: DataStream[Long] = fizzbuzz.filter(_.result == "fizzbuzz")
      .map(_.number)
      .setParallelism(1)

    //      fizzBuzzNumbers.writeAsText("src/main/resources/output/fizzbuzz")
    //        .setParallelism(1)

    // add a sink for replacement of writeAsText
    // older version before 1.13
    //      fizzbuzz.addSink(
    //        StreamingFileSink.forRowFormat[String](
    //          new org.apache.flink.core.fs.Path("src/main/resources/output/fizzbuzz_sink"),
    //          new SimpleStringEncoder[String]("UTF-8")
    //        ).build()
    //      )
    val sink = FileSink.forRowFormat(
      new org.apache.flink.core.fs.Path("src/main/resources/output/fizzbuzz"),
      new SimpleStringEncoder[Long]("UTF-8")
    ).build()
    fizzBuzzNumbers.sinkTo(sink).setParallelism(1)
    env.execute()
  }

  def explicitTransformations(): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    val numbers: DataStream[Long] = env.fromSequence(1, 100)
    val doubledNumbers = numbers.map(n => n * 2)

    // explicit transformations
    val doubledNumbersV2 = numbers.map(new MapFunction[Long, Long] {
      // you can declare fields, methods, etc.
      override def map(value: Long): Long = value * 2
    })

    val expandedNumbers = numbers.flatMap(n => Range.Long(1, n, 1).toList)

    // explicit
    val expandedNumbresV2 = numbers.flatMap(new FlatMapFunction[Long, Long] {
      // declare fields, methods etc.
      override def flatMap(value: Long, collector: Collector[Long]): Unit =
        Range.Long(1, value, 1).foreach { in =>
          collector.collect(in) // imperative style
        }
    })

    // process method
    val expandedNumbersV3 = numbers.process(new ProcessFunction[Long, Long] {
      override def processElement(n: Long, context: ProcessFunction[Long, Long]#Context, collector: Collector[Long]): Unit =
        Range.Long(1, n, 1).foreach { in =>
          collector.collect(in)
        }
    })

    // reduce
    // happens on keyed streams
    val keyedNumbers: KeyedStream[Long, Boolean] = numbers.keyBy(n => n % 2 == 0)
    // reduce functional style
    val sumByKey = keyedNumbers.reduce(_ + _)

    // reduce imperative
    val sumByKeyV2 = keyedNumbers.reduce(new ReduceFunction[Long] {
      override def reduce(n1: Long, n2: Long): Long = n1 + n2
    })

    sumByKeyV2.print()
    env.execute()
  }

  def main(args: Array[String]): Unit = {
    //    applicationTemplate()
    //    demoTransofrmations()
    //    fizzBuzz()
    explicitTransformations()
  }
}
