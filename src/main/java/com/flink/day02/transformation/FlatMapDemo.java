package com.flink.day02.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/2/13 13:18
 * @desc 数据为一条转换为三条，显然，应当使用flatMap来实现分别在flatMap函数中构建三个数据，并放入到一个列表中
 */
public class FlatMapDemo {
    public static void main(String[] args) throws Exception {
        //1 .env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<String> lines = env.readTextFile("./data/input/flatmap.log");
        //3. transformation
        SingleOutputStreamOperator<String> result = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String lines, Collector<String> out) throws Exception {
                String[] elements = lines.split(",");
                // one element --> three elements
                out.collect(elements[0] + "hive" + elements[1]);
                out.collect(elements[0] + "hive" + elements[2]);
                out.collect(elements[0] + "hive" + elements[3]);
            }
        });
        //4. sink
        result.print();
        //5. execute
        env.execute("FlatMapDemo");
    }
}
