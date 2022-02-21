package com.flink.day02.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/21 21:43
 * @desc
 */
public class WriteSinkDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        int parallelism0 = env.getParallelism();
        System.out.println("local env parallelism:" + parallelism0);
        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
        int parallelism = lines.getParallelism();
        System.out.println("source parallelism:" + parallelism);
        //3. transform
        //4. sink
        lines.writeAsText("./data/output/3", FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        //5. execute
        env.execute();
    }
}
