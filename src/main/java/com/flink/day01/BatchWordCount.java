package com.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/2/6 21:37
 * @desc * todo flink1.12之前的写法
 * 编写Flink程序，读取文件中的字符串，并以空格进行单词拆分打印。
 * 实现步骤：
 * 1）获取flink批处理的运行环境
 * 2）接入数据源，读取文件获取数据
 * 3）数据处理
 * 3.1：使用flatMap对单词进行拆分
 * 3.2：对拆分后的单词进行记一次数
 * 3.3：使用分组算子对key进行分组
 * 3.4：对分组后的key进行聚合操作
 * 4）构建sink，输出结果
 */
public class BatchWordCount {
    public static void main(String[] args) throws Exception {

//        1）获取flink批处理的运行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        2）接入数据源，读取文件获取数据
        DataSource<String> lines = env.readTextFile("./data/input/wordcount.txt");
//        3）数据处理
//        3.1：使用flatMap对单词进行拆分
        FlatMapOperator<String, String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String lines, Collector<String> out) throws Exception {
                String[] words = lines.split(" ");
                for (String word : words) {
                    out.collect(word);
                }
            }
        });
//        3.2：对拆分后的单词进行记一次数
        MapOperator<String, Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });
//        3.3：使用分组算子对key进行分组
        UnsortedGrouping<Tuple2<String, Integer>> grouped = wordAndOne.groupBy(0);
//        3.4：对分组后的key进行聚合操作
        AggregateOperator<Tuple2<String, Integer>> sumed = grouped.sum(1);
//        4）构建sink，输出结果
        sumed.print();

    }
}
