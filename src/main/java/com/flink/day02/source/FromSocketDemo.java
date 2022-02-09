package com.flink.day02.source;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/2/9 22:25
 * @desc 编写Flink程序，接收socket的单词数据，并以空格进行单词拆分打印。
 */
public class FromSocketDemo {
    public static void main(String[] args) throws Exception {
        //0. env
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8089);  //设置端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        System.out.println("执行环境默认的并行度：" + env.getParallelism());

        // 1. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
        System.out.println("socketTextStream的并行度：" + lines.getParallelism());

        // 2. transform
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String lines, Collector<String> out) throws Exception {
                String[] words = lines.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        }).setParallelism(2);
        System.out.println("words的并行度：" + words.getParallelism());

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
        System.out.println("wordAndOne的并行度：" + wordAndOne.getParallelism());

        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(t -> t.f0).sum(1);
        System.out.println("sumed的并行度：" + sumed.getParallelism());

        // 3. sink
        sumed.print();

        // 4. execute
        env.execute("FromSocketDemo");
    }
}
