package com.flink.day02.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/12 21:23
 * @desc 读取 apache.log 文件中的访问日志数据，过滤出来以下访问IP ：83.149.9.216 的访问日志数据
 */
public class FilterDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<String> lines = env.readTextFile("./data/input/apache.log");
        //3. transformation
        SingleOutputStreamOperator<String> result = lines.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String lines) throws Exception {
                return lines.contains("83.149.9.216");
            }
        });
        //4. sink
        result.print();
        //5. execute
        env.execute("FilterDemo");

    }
}
