package com.flink.day02.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/13 14:36
 * @desc socket test, min value and max value
 */
public class MinByAndMaxByDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
        //3. transformation, minBy and Maxby
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String lines) throws Exception {
                String[] words = lines.split(" ");
                String word = words[0];
                Integer count = Integer.parseInt(words[1]);
                return Tuple2.of(word, count);
            }
        });

        KeyedStream<Tuple2<String, Integer>, Object> keyed = wordAndOne.keyBy(w -> w.f0);

        keyed.minBy(1).print("最小的数据>>>");
        keyed.maxBy(1).print("最大的数据>>>");

        //4. sink
        //5. execute
        env.execute();
    }
}
