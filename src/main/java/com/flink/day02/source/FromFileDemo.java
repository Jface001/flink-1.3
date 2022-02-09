package com.flink.day02.source;

import org.apache.flink.api.java.io.TextInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.FileProcessingMode;

/**
 * @author jface
 * @create 2022/2/9 22:14
 * @desc
 */
public class FromFileDemo {
    public static void main(String[] args) throws Exception {
        // 1. env
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);  //设置端口号
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        String path = "./data/input/wordcount.txt";
        DataStreamSource<String> lines = env.readTextFile(path);
        DataStreamSource<String> lines2 = env.readFile(new TextInputFormat(null), path, FileProcessingMode.PROCESS_ONCE, 2000);
        // 每隔 2s 重新读取一次该文件中的内容，作业不会停止，是流处理的应用
        DataStreamSource<String> lines3 = env.readFile(new TextInputFormat(null), path, FileProcessingMode.PROCESS_CONTINUOUSLY, 2000);

        System.out.println("readTextFile创建的DataStream并行度为：" + lines.getParallelism());
        System.out.println("readFile创建的DataStream并行度为：" + lines2.getParallelism());
        System.out.println("readFile2创建的DataStream并行度为：" + lines3.getParallelism());

        lines.print();
        lines2.print();
        lines3.print();

        env.execute();
    }
}
