package com.flink.day04.watermark.kafka;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.internals.Topic;

import java.util.Properties;

/**
 * kafka生产者工具类，模拟数据的生成，将数据写入到指定的分区中
 *
 * 第一个分区写入：1000,hadoop、7000,hadoop-》没有触发窗口计算
 * 第二个分区写入：7000,flink              -》触发了窗口计算
 * 发现一个问题，就是当某一个分区的触发机制达到的时候，其他的分区触发机制迟迟未触发的时候，我们窗口不能被触发计算，这样的话，假如数据倾斜比较严重
 * 某个分区的数据量很大，或者说某个分区一直都有数据产生，其他的分区迟迟没有更新watermark，这个时候就会出现问题，窗口无法被触发计算，
 * 极端情况下，等很久很久都不会触发计算
 *
 * 这个时候就应该需要用到方法：withIdleness(Duration.ofSeconds(30))
 *
 * 结论：
 * 使用withIdleness，其实就是当某个分区的窗口满足了触发条件，其他的分区没有数据或者没有触发窗口计算的时候，持续按照设置好的空闲时间进行窗口的计算操作
 * 如果一直有数据但是都没有达到触发条件的话，窗口不会触发计算
 * 这个值一般设置多少合适呢？ 一般认为根据业务场景来分析，如果数据量比较小，并且倾斜比较严重的话，可以设置小一些，如果数据量比较大并且很难出现一个分区或者多个分区
 * 迟迟无数据的情况，那么可以设置的大一些
 *
 * 总之一般设置1-10分钟以内为佳。
 */
public class KafkaMock {
    private final KafkaProducer<String, String> producer;
    public final static String TOPIC = "test3";

    private KafkaMock(){
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node01:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<String, String>(props);
    }
    public void producer(){
        long timestamp = 1000;
        String value = "hadoop";
        String key = String.valueOf(value);
        String data = String.format("%s,%s", timestamp, value);
        producer.send(new ProducerRecord<String, String>(TOPIC, 1, key, data));
        producer.close();
    }
    public static void main(String[] args) {
        new KafkaMock().producer();
    }
}
