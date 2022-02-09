package com.flink.day02.source;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/9 22:10
 * @desc 使用env.fromElements()，这种方式也支持Tuple，自定义对象等复合形式。
 * fromElements不可以支持多个并行度
 */
public class FromElementDemo {
    public static void main(String[] args) throws Exception {

        //1. env
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);  //设置端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        DataStreamSource<Integer> nums = env.fromElements(1, 2, 3, 4, 5, 6, 7);
        DataStreamSource<Tuple2<String, Integer>> tuple = env.fromElements(new Tuple2<>("spark", 1), new Tuple2<>("flink", 2));

        System.out.println("FromElement的并行度为：" + nums.getParallelism());
        nums.print();
        tuple.print();
        env.execute();

    }
}
