package com.flink.day02.transformation;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.text.SimpleDateFormat;

/**
 * @author jface
 * @create 2022/2/13 13:59
 * @desc use map transformation, .log --> JavaBean
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<String> lines = env.readTextFile("./data/input/apache2.log");
        //3. transformation
        SingleOutputStreamOperator<ApacheEvent> apacheEventBean = lines.map(new RichMapFunction<String, ApacheEvent>() {
            @Override
            public ApacheEvent map(String lines) throws Exception {
                SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss");
                String[] elements = lines.split(" ");
                String ip = elements[0];
                int userId = Integer.parseInt(elements[1]);
                long timestamp = simpleDateFormat.parse(elements[2]).getTime();
                String method = elements[3];
                String path = elements[4];
                return new ApacheEvent(ip, userId, timestamp, method, path);
            }
        });
        //4. sink
        apacheEventBean.print();
        //5. execute
        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class ApacheEvent {
        String ip;
        int userId;
        long timestamp;
        String method;
        String path;
    }
}
