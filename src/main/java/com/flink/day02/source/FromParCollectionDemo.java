package com.flink.day02.source;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/9 22:19
 * @desc 使用env.generateSequence()方法创建基于Sequence的DataStream
 * 使用env.fromSequence()方法创建基于开始和结束的DataStream
 */
public class FromParCollectionDemo {
    public static void main(String[] args) throws Exception {
        // 1. env
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8098);  //设置端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //2. 设置全局变量
        env.setParallelism(10);

        DataStreamSource<Long> nums = env.generateSequence(1, 10);
        DataStreamSource<Long> nums2 = env.generateSequence(1, 10).setParallelism(2);
        DataStreamSource<Long> nums3 = env.fromSequence(1, 10);


        System.out.println("nums创建的DataStream的并行度为：" + nums.getParallelism());
        System.out.println("nums2创建的DataStream的并行度为：" + nums2.getParallelism());
        System.out.println("nums3创建的DataStream的并行度为：" + nums3.getParallelism());
        nums.print();
        //nums2.print();
        //nums3.print();
        env.execute();

    }
}
