package com.flink.day03.connector;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.util.Properties;

/**
 * @author jface
 * @create 2022/2/23 22:53
 * @desc source --> kafka
 */
public class KafkaProducer {
    public static void main(String[] args) throws Exception {
        //1. env
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        env.enableCheckpointing(5000);

        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        //3. transform

        //4. sink
        String topicName = "test";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "node1:9092");
        FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(topicName, new SimpleStringSchema(), properties);

        lines.addSink(myProducer);

        //5. execute
        env.execute();

    }
}
