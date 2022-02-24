package com.flink.day03.connector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisClusterConfig;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/2/24 21:22
 * @desc 从指定的socket读取数据，对单词进行计算，将结果写入到Redis中
 */
public class RedisSinkDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //启动检查点（每隔5秒钟生成一个栅栏，然后栅栏会发送给job作业的第一个算子（source），当source收到栅栏以后，会阻塞当前算子的计算任务，
        // 然后将当前的计算结果state的数据持久化的存储起来，然后栅栏会随着数据的流动进入到下一个算子，继续刚才的快照操作，依次类推，进入到最后一个算子（sink），
        // 当sink算子收到栅栏以后，会将当前的计算结果写入到mysql中）
        // 为什么checkpoint时候才会将计算结果写入到mysql?
        //  一次性语义！exactly-once
        env.enableCheckpointing(5000);
        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        //3. transform
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String lines, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = lines.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndOne.keyBy(t -> t.f0).sum(1);

        //4. sink
        result.print();

        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder()
                .setHost("node03")
                .setDatabase(0)
                .build();

        result.addSink(new RedisSink<Tuple2<String, Integer>>(config, new RedisWordCountMapper()));

        //5. execute
        env.execute();

    }

    /**
     * 自定义redis写入的实现类，继承自RedisMapper
     */
    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {

        /**
         * redis datatype
         *
         * @return
         */
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(
                    RedisCommand.HSET, "wordcount"
            );
        }

        /**
         * key
         *
         * @param data
         * @return
         */
        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }

        /**
         * value
         *
         * @param data
         * @return
         */
        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }

}
