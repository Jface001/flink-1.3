package com.flink.day03.connector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.kafka.shaded.org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * @author jface
 * @create 2022/2/23 22:37
 * @desc 需求：使用flink-connector-kafka-2.11中的FlinkKafkaConsumer消费kafka中的数据进行wordcount计算
 * 需要设置的参数：
 * 1：主题名称
 * 2：反序列化的规则
 * 3：消费者属性-集群地址
 * 4：消费者属性-消费者组id（如果不设置，会有默认的消费者组id，但是默认的不方便管理）
 * 5：消费者属性-offset重置规则
 * 6：动态分区检测（当kafka的分区数量发生变化，flink能够感知到）
 * 7：如果没有开启checkpoint，那么可以设置自动递交offset
 * 如果开启了checkpoint，checkpoint会将kafka的offset随着checkpoint成功的时候递交到默认的主题中
 */
public class KafkaConsumer {
    public static void main(String[] args) throws Exception {
        //1. env
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        //2. source
        String topicName = "test";

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "node02:9092");
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "test001");
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "TRUE");
        properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
        properties.setProperty("flink.partition-discovery.interval-millis", "5000");// 动态分区检测
        //kafkaSource.setStartFromEarliest();     // 尽可能从最早的记录开始
        //kafkaSource.setStartFromLatest();       // 从最新的记录开始
        //kafkaSource.setStartFromTimestamp(...); // 从指定的时间开始（毫秒）
        //kafkaSource.setStartFromGroupOffsets(); // 默认的方法
        FlinkKafkaConsumer<String> kafkaSource = new FlinkKafkaConsumer<>(topicName, new SimpleStringSchema(), properties);

        kafkaSource.setCommitOffsetsOnCheckpoints(true);//在开启checkpoint以后，offset的递交会随着checkpoint的成功而递交，从而实现一致性语义，默认就是true

        DataStreamSource<String> kafkaDS = env.addSource(kafkaSource);

        //3. transform
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = kafkaDS.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = value.split(" ");
                for (String word : words) {
                    out.collect(Tuple2.of(word, 1));
                }
            }

        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = wordAndOne.keyBy(t -> t.f0).sum(1);


        //4. sink
        result.print();

        //5. execute
        env.execute();

    }
}
