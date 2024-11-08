package com.github.rtjvm.part2datastreams

import com.github.rtjvm.gaming._
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.{AllWindowFunction, ProcessAllWindowFunction, ProcessWindowFunction, WindowFunction}
import org.apache.flink.streaming.api.windowing.assigners.{EventTimeSessionWindows, GlobalWindows, SlidingEventTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger
import org.apache.flink.streaming.api.windowing.windows.{GlobalWindow, TimeWindow}
import org.apache.flink.util.Collector

import java.time.Instant
import scala.concurrent.duration.DurationInt

object WindowFunctions {
  // use -case: stream of events for gaming session

  val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

  implicit val serverStartTime: Instant = Instant.parse("2022-02-02T00:00:00.000Z")

  val events: List[ServerEvent] = List(
    bob.register(2.seconds), // player bob registered 2s after the server started
    bob.online(5.seconds),
    sam.register(3.seconds),
    sam.online(4.seconds),
    rob.register(4.seconds),
    alice.register(4.seconds),
    mary.register(6.seconds),
    mary.online(6.seconds),
    carl.register(8.seconds),
    rob.online(10.seconds),
    alice.online(10.seconds),
    carl.online(10.seconds)
  )

  // how many players were registered every 3 seconds?
  val eventStream: DataStream[ServerEvent] = env
    .fromCollection(events)
    .assignTimestampsAndWatermarks(
      // specify how to extract timestamps for events (event time) + watermark
      WatermarkStrategy
        .forBoundedOutOfOrderness(java.time.Duration.ofMillis(500)) // once you get event at time T, you will not accept data older than T - 500ms
        .withTimestampAssigner(new SerializableTimestampAssigner[ServerEvent] {
          override def extractTimestamp(event: ServerEvent, recordTimestamp: Long): Long =
            event.eventTime.toEpochMilli
        })
    )
  val threeSecondsTumblingWindow = eventStream.windowAll(TumblingEventTimeWindows.of(Time.seconds(3)))

  /*
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
    |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |                                     |                                             |                                           |                                    |
    |            1 registrations          |               3 registrations               |              2 registration               |            0 registrations         |
    |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
 */

  // count by windowAll
  // AllWindowFunction takes three types: Input type, output type and the window type
  class CountByWindowAll extends AllWindowFunction[ServerEvent, String, TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}]: $registrationEventCount registrations")
    }
  }

  def demoCountByWindow(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.apply(new CountByWindowAll)
    registrationsPerThreeSeconds.print()
    env.execute()
  }


  // rocher option
  class CountByWindowAllV2 extends ProcessAllWindowFunction[ServerEvent, String, TimeWindow] {
    // Context is much richer data structure than Collector
    override def process(context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window.getStart} - ${window.getEnd}]: $registrationEventCount registrations")
    }
  }

  def demoCountByWindowV2(): Unit = {
    val registrationsPerThreeSeconds: DataStream[String] = threeSecondsTumblingWindow.process(new CountByWindowAllV2)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  // aggregate function to calculate the number of registrations
  // parameter types are: input type, accumulator type, output type
  class CountByWindowV3 extends AggregateFunction[ServerEvent, Long, Long] {
    // initial accumulator value
    override def createAccumulator(): Long = 0L

    // how to add an event to the accumulator
    override def add(value: ServerEvent, accumulator: Long): Long = {
      if (value.isInstanceOf[PlayerRegistered]) accumulator + 1
      else accumulator
    }

    // what to return as the result
    override def getResult(accumulator: Long): Long = accumulator

    // how to merge two accumulators
    override def merge(a: Long, b: Long): Long = a + b
  }

  def demoCountByWindowV3(): Unit = {
    val registrationsPerThreeSeconds: DataStream[Long] = threeSecondsTumblingWindow.aggregate(new CountByWindowV3)
    registrationsPerThreeSeconds.print()
    env.execute()
  }

  /*
   * Keyed Streams and Window functions
   */
  // each element will be assigned to mini-stream for its own key
  val streamByType: KeyedStream[ServerEvent, String] = eventStream.keyBy(event => event.getClass.getSimpleName)

  // for every key, have seprate window created
  val threeSecondsTumblingWindowsByType = streamByType.window(TumblingEventTimeWindows.of(Time.seconds(3)))

  /*
    === Registration Events Stream ===
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob registered  | sam registered  | rob registered    |       | mary registered |       | carl registered |     |               |              |
    |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |            1 registration           |               3 registrations               |              2 registrations              |            0 registrations         |
    |     1643760000000 - 1643760003000   |        1643760003000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |

    === Online Events Stream ===
    |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
    |         |         | bob online      |                 | sam online        |       | mary online     |       |                 |     | rob online    | carl online  |
    |         |         |                 |                 |                   |       |                 |       |                 |     | alice online  |              |
    ^|------------ window one ----------- + -------------- window two ----------------- + ------------- window three -------------- + ----------- window four ----------|^
    |            1 online                 |               1 online                      |              1 online                     |            3 online                |
    |     1643760000000 - 1643760003000   |        1643760005000 - 1643760006000        |       1643760006000 - 1643760009000       |    1643760009000 - 1643760012000   |
  */
  // input type, output type, key type and window type
  class CountByWindow extends WindowFunction[ServerEvent, String, String, TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      out.collect(s"Key: $key, Window $window, Size: ${input.size}")
    }
  }

  def demoCountByTypeByWindow(): Unit = {
    threeSecondsTumblingWindowsByType.apply(new CountByWindow).print()
    env.execute()
  }

  class CountByWindowV2 extends ProcessWindowFunction[ServerEvent, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val window = context.window
      val registrationEventCount = elements.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Key: $key, Window [${window.getStart} - ${window.getEnd}]: $registrationEventCount registrations")
    }
  }

  def demoCountByTypeByWindowV2(): Unit = {
    threeSecondsTumblingWindowsByType.process(new CountByWindowV2).print()
    env.execute()
  }

  /*
  sliding windows
  How many players were registered every 3 seconds, updated every 1 second
   */

  // how many players were registered every 3 seconds, UPDATED EVERY 1s?
  // [0s...3s] [1s...4s] [2s...5s] ...

  /*
  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    | carl online  |
  |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
  ^|------------ window one ----------- +
                1 registration

            + ---------------- window two --------------- +
                                  2 registrations

                       + ------------------- window three ------------------- +
                                           4 registrations

                                         + ---------------- window four --------------- +
                                                          3 registrations

                                                          + ---------------- window five -------------- +
                                                                           3 registrations

                                                                               + ---------- window six -------- +
                                                                                           1 registration

                                                                                        + ------------ window seven ----------- +
                                                                                                    2 registrations

                                                                                                         + ------- window eight------- +
                                                                                                                  1 registration

                                                                                                                + ----------- window nine ----------- +
                                                                                                                        1 registration

                                                                                                                                   + ---------- window ten --------- +
                                                                                                                                              0 registrations
   */
  def demoSlidingAllWindows(): Unit = {
    val windowSize: Time = Time.seconds(3)
    val slidingTime: Time = Time.seconds(1)

    val slidingWindowsAll = eventStream.windowAll(SlidingEventTimeWindows.of(windowSize, slidingTime))
    // process streams of data with similar window functions
    val registrationCountByWindow = slidingWindowsAll.apply(new CountByWindowAll)
    registrationCountByWindow.print()
    env.execute()
  }

  /**
   * Session windows = Group of events with no more than certain time gap in between them
   * How many registration events do we have no more than 1 seconds apart?
   */
  /*
  |----0----|----1----|--------2--------|--------3--------|---------4---------|---5---|--------6--------|---7---|--------8--------|--9--|------10-------|------11------|
  |         |         | bob registered  | sam registered  | sam online        |       | mary registered |       | carl registered |     | rob online    |              |
  |         |         | bob online      |                 | rob registered    |       | mary online     |       |                 |     | alice online  |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     | carl online   |              |

  after filtering:

  +---------+---------+-----------------+-----------------+-------------------+-------+-----------------+-------+-----------------+-----+---------------+--------------+
  |         |         | bob registered  | sam registered  | rob registered    |       | mary registered |       | carl registered |     |     N/A       |              |
  |         |         |                 |                 | alice registered  |       |                 |       |                 |     |               |              |
                      ^ ----------------- window 1 -------------------------- ^       ^ -- window 2 --- ^       ^ -- window 3 --- ^     ^ -- window 4 - ^
*/


  def demoSessionWindows(): Unit = {
    val groupBySessionWindows = eventStream.windowAll(EventTimeSessionWindows.withGap(Time.seconds(1)))
    groupBySessionWindows.apply(new CountByWindowAll).print()
    env.execute()
  }

  /**
   * Global Windows = All elements are assigned to the same window
   * How many registrations do we have every 10 events
   */

  class CountByGlobalWindowAll extends AllWindowFunction[ServerEvent, String, GlobalWindow] {
    override def apply(window: GlobalWindow, input: Iterable[ServerEvent], out: Collector[String]): Unit = {
      val registrationEventCount = input.count(event => event.isInstanceOf[PlayerRegistered])
      out.collect(s"Window [${window}]: $registrationEventCount registrations")
    }
  }

    def demoGlobalWindow(): Unit = {
      val globalWindowEvents = eventStream.windowAll(GlobalWindows.create())
        .trigger(CountTrigger.of[GlobalWindow](10))
        .apply(new CountByGlobalWindowAll)
      globalWindowEvents.print()
      env.execute()
    }

  /**
   * What was the time window (continuous 2s) when we had the most number of registration events?
   * - What kind of window function? -> AllWindowFunction
   * - What kind of windows should we use? -> Sliding Windows
   */

  class KeepWindowAndCountFunction extends AllWindowFunction[ServerEvent, (TimeWindow, Long), TimeWindow] {
    override def apply(window: TimeWindow, input: Iterable[ServerEvent], out: Collector[(TimeWindow, Long)]): Unit =
      out.collect((window, input.size))
  }

  def windowFunctionsExercise(): Unit = {
    val slidingWindow: DataStream[(TimeWindow, Long)] = eventStream.filter(_.isInstanceOf[PlayerRegistered])
      .windowAll(SlidingEventTimeWindows.of(Time.seconds(2), Time.seconds(1)))
      .apply(new KeepWindowAndCountFunction)

    val localWindows: List[(TimeWindow, Long)] = slidingWindow.executeAndCollect().toList
    val maxwindow: (TimeWindow, Long) = localWindows.maxBy(_._2)
    println(s"The best window is ${maxwindow._1} with registration events ${maxwindow._2}")

  }

  def main(args: Array[String]): Unit = {
//    demoCountByWindow()
//    demoCountByWindowV2()
//    demoCountByWindowV3()

//    demoCountByTypeByWindow()
//    demoCountByTypeByWindowV2()

//    demoSlidingAllWindows()

//    demoSessionWindows()

//    demoGlobalWindow()

    windowFunctionsExercise()
  }
}
