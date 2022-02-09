package com.flink.day02.source;


import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 * @author jface
 * @create 2022/2/8 22:51
 * @desc
 * 使用env.fromCollection()，这种方式也支持Tuple，自定义对象等复合形式。
 *  fromCollection不可以支持多个并行度
 */
public class FromCollectionDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);  //设置端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //The parallelism of non parallel operator must be 1.(fromElements不可以支持多个并行度)
        // DataStreamSource<Integer> nums = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7)).setParallelism(2);
        DataStreamSource<Integer> nums = env.fromCollection(Arrays.asList(1, 2, 3, 4, 5, 6, 7));
        DataStreamSource<Tuple2<String, Integer>> tuple = env.fromCollection(
                Arrays.asList(
                        new Tuple2<>("spark", 2),
                        new Tuple2<>("hadoop", 2)
                ));

        System.out.println("FromCollectionDemo设置的并行度为：" + nums.getParallelism());
        nums.print();
        tuple.print();
        env.execute();

    }
}
