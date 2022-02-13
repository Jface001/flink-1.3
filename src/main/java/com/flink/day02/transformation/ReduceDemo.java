package com.flink.day02.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/13 15:01
 * @desc
 *  read apache.log,   calculate PV
 */
public class ReduceDemo {
    public static void main(String[] args) throws Exception {
        // 1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. source
        DataStreamSource<String> lines = env.readTextFile("./data/input/apache.log").setParallelism(1);
        // 3. transformation
        SingleOutputStreamOperator<Tuple2<String, Integer>> ipAndOne = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                String[] dataArray = value.split(" ");
                return Tuple2.of(dataArray[0], 1);
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = ipAndOne.keyBy(t -> t.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        });
        // 4. sink
        result.print().setParallelism(1);
        // 5. execute
        env.execute();

    }
}
