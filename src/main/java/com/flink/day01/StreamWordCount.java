package com.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/2/6 22:05
 * @desc todo flink1.12之前的写法
 * 编写Flink程序，接收socket的单词数据，并以空格进行单词拆分打印。
 */
public class StreamWordCount {
    public static void main(String[] args) throws Exception {
//        1）获取flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        2）接入数据源，读取文件获取数据
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
//        3）数据处理
//        3.1：使用flatMap对单词进行拆分
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String lines, Collector<String> out) throws Exception {
                String[] words = lines.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
//        3.2：对拆分后的单词进行记一次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
//        3.3：使用分组算子对key进行分组
        KeyedStream<Tuple2<String, Integer>, Tuple> grouped = wordAndOne.keyBy(0);
//        3.4：对分组后的key进行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> result = grouped.sum(1);
//        4）构建sink，输出结果
        result.print();
//        5）执行任务
        env.execute();


    }
}
