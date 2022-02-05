package com.flink.day01;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @author jface
 * @create 2022/2/5 18:28
 * @desc 编写Flink程序，读取文件中的字符串，并以空格进行单词拆分打印
 */
public class BatchWordCountToYarn {
    public static void main(String[] args) throws Exception {
        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String output = "";
        if (parameterTool.has("output")) {
            output = parameterTool.get("output");
            System.out.println("指定了输出路径:" + output);
        } else {
            output = "hdfs://node1::8020/flink/output";
            System.out.println("没有指定输出路径，默认输出路径为:" + output);
        }

        //TODO: 0.env
        //ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //env.setRuntimeMode(RuntimeExecutionMode.BATCH);// 使用 DataSteam 实现批处理
        //env.setRuntimeMode(RuntimeExecutionMode.STREAMING); // 使用 DataStream 实现流处理
        //env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC); // 使用 DataStream 根据数据源自动选择使用流还是批
        //TODO: 1.source
        DataStreamSource<String> line = env.fromElements("flink hadoop spark", "hadoop flink spark", "spark flink hadoop", "flink");

        //TODO: 2.transformation
        // 切割
        SingleOutputStreamOperator<String> words = line.flatMap(
                (String value, Collector<String> out) -> Arrays.stream(value.split(" ")).forEach(out::collect)
        ).returns(Types.STRING);
        // 分组
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(
                (String value) -> Tuple2.of(value, 1)
        ).returns((Types.TUPLE(Types.STRING, Types.INT)));

        // 聚合
        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = grouped.sum(1);

        //TODO: 3.sink
        //如果执行报hdfs权限相关错误,可以执行 hadoop fs -chmod -R 777  /
        System.setProperty("HADOOP_USER_NAME", "root");// 设置用户名
        result.writeAsText(output + System.currentTimeMillis());

        //TODO: 4.execute / 执行并等待程序结束
        env.execute();


    }
}
