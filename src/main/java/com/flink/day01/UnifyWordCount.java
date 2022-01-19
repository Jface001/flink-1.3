package com.flink.day01;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/1/19 23:34
 * @desc  使用流批一体 API 编程模型实现单词计算(flink 1.12 以后版本可实现)
 * 在flink中批是流的一个特例，也就意味着不管实现批还是流处理，肯定按照流的api实现批处理
 * DataStream
 * StreamExecutionEnvironment
 */
public class UnifyWordCount {
    public static void main(String[] args) {
        //1）获取flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2）接入数据源，读取文件获取数据

        //3）数据处理
        //  3.1：使用flatMap对单词进行拆分
        //  3.2：对拆分后的单词进行记一次数
        //  3.3：使用分组算子对key进行分组
        //  3.4：对分组后的key进行聚合操作
        //4）构建sink，输出结果


    }
}
