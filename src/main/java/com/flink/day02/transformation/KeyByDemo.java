package com.flink.day02.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/2/13 13:53
 * @desc socket >> wordcount
 */
public class KeyByDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
        //3. transformation, ByKey
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String lines, Collector<String> out) throws Exception {
                String[] words = lines.split(" ");
                for (String word : words) {
                    out.collect(word);
                }

            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String words) throws Exception {
                return Tuple2.of(words, 1);
            }
        });

        // keyBy datatype is Tuple2 or JavaBean
        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = grouped.sum(1);

        //4. sink
        sumed.print();

        //5. execute
        env.execute();
    }
}
