package com.flink.day02.transformation;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.stream.Stream;

/**
 * @author jface
 * @create 2022/2/13 15:32
 * @desc
 * union stream
 * condition: 1. same datatype   2. result no distinct
 */
public class UnionDemo {
    public static void main(String[] args) throws Exception {
        // 1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 2. source
        DataStream<String> ds1 = env.fromElements("hadoop", "hive", "flume");
        DataStream<String> ds2 = env.fromElements("hadoop", "hive", "spark");
        // 3. transformation
        DataStream<String> result = ds1.union(ds2);
        // 4. sink
        result.print();
        // 5. execute
        env.execute("union demo");

    }
}
