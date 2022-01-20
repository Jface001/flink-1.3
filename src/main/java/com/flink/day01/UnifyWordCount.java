package com.flink.day01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/1/19 23:34
 * @desc  使用流批一体 API 编程模型实现单词计算(flink 1.12 以后版本可实现)
 * 在flink中批是流的一个特例，也就意味着不管实现批还是流处理，肯定按照流的api实现批处理
 * DataStream
 * StreamExecutionEnvironment
 */
public class UnifyWordCount {
    public static void main(String[] args)  throws Exception {
        //1）获取flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2）接入数据源，读取文件获取数据
        env.setRuntimeMode(RuntimeExecutionMode.BATCH); //使用 dataStream 实现批处理
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING);//使用 dataStream 实现批处理，如果数据源有界，则依然是批处理
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);//使用 dataStream 根据数据源自动选择批还是流
        DataStreamSource<String> lines = env.readTextFile("./data/input/wordcount.txt");

        //3）数据处理
        //  3.1：使用flatMap对单词进行拆分
        SingleOutputStreamOperator<String> word = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(" ");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        });
        //  3.2：对拆分后的单词进行记一次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = word.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        //  3.3：使用分组算子对key进行分组
       /* KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> value) throws Exception {
                return value.f0
            }
        });*/
        // 简写方式
        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(t -> t.f0);
        //  3.4：对分组后的key进行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = grouped.sum(1);
        //4）构建sink，输出结果
        sumed.print();
        //5) 启动运行
        env.execute();


    }
}
