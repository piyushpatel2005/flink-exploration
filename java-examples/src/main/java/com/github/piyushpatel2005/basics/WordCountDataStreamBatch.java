package com.github.piyushpatel2005.basics;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

public class WordCountDataStreamBatch {
    public static void main(String[] args) throws Exception {
        // Initialize the context and the environment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // bin/flink run -Dexecution.runtime-mode=BATCH
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<String> dataStreamSource = env.readTextFile("input/words.txt");

//        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndCount = dataStreamSource.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
//            @Override
//            public void flatMap(String line, Collector<Tuple2<String, Long>> out) throws Exception {
//                Arrays.stream(line.split("\\s+"))
//                        .forEach(word -> out.collect(Tuple2.of(word, 1L)));
//            }
//        });

        var wordAndCount = dataStreamSource
                .flatMap((String line, Collector<Tuple2<String, Long>> out) -> Arrays.stream(line.split("\\s+"))
                .forEach(word -> out.collect(Tuple2.of(word, 1L))))
                .returns(Types.TUPLE(Types.STRING, Types.LONG));

        KeyedStream<Tuple2<String, Long>, String> keyedStream = wordAndCount.keyBy(pair -> pair.f0);
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyedStream.sum(1);

        sum.print();
        env.execute();
    }
}
