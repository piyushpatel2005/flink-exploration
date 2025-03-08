package com.github.piyushpatel2005.basics;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.stream.Stream;

public class WordCountStreaming {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // nc -lk 4567
//        env.socketTextStream("localhost", 4567)
//                .flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
//
//                    @Override
//                    public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
//                        Arrays.stream(line.split("\\s+"))
//                                .forEach(word -> out.collect(Tuple2.of(word, 1L)));
//                    }
//                }).keyBy(pair -> pair.f0)
//                .sum(1)
//                .print();
        env.socketTextStream("localhost", 4567)
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> Arrays.stream(line.split("\\s+"))
                        .map(word -> Tuple2.of(word, 1L))
                        .forEach(out::collect))
                        .returns(Types.TUPLE(Types.STRING, Types.LONG))
                        .keyBy(pair -> pair.f0)
                        .sum(1)
                        .print();
        env.execute();

    }
}
