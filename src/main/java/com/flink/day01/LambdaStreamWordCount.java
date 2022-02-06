package com.flink.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;


import java.util.Arrays;

/**
 * @author jface
 * @create 2022/2/6 21:47
 * @desc todo flink1.12之前的写法
 * 编写Flink程序，接收socket的单词数据，并以空格进行单词拆分打印。
 */
public class LambdaStreamWordCount {
    public static void main(String[] args) throws Exception {
//        1）获取flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        2）获取socket的数据源
        DataStreamSource<String> lines = env.socketTextStream("127.0.0.1", 9999);
//        3）数据处理
//        3.1：使用flatMap对单词进行拆分
        SingleOutputStreamOperator<String> words = lines.flatMap(
                (String line, Collector<String> out) -> Arrays.stream(line.split(" ")).forEach(out::collect)
        ).returns(Types.STRING);
//        3.2：对拆分后的单词进行记一次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(w -> Tuple2.of(w, 1)).returns(Types.TUPLE(Types.STRING, Types.INT));
//        3.3：使用分组算子对key进行分组
        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(t -> t.f0);
//        3.4：对分组后的key进行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = grouped.sum(1);
//        4）构建sink，输出结果
        sumed.print();
//        5）执行任务
        env.execute();

    }
}
