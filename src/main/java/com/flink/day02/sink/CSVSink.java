package com.flink.day02.sink;

import org.apache.flink.api.java.tuple.Tuple7;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/20 17:28
 * @desc
 *  dataSet >>>> csvSink
 */
public class CSVSink {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        String filePath ="./data/output/user.csv";
        Tuple7<Integer, String, Integer, Integer, String, String, Long> row = new Tuple7<>(15, "zhangsan", 40, 1, "CN", "2020-09-08 00:00:00", 1599494400000L);

        //3. transform
        DataStreamSource<Tuple7<Integer, String, Integer, Integer, String, String, Long>> dataSet = env.fromElements(row);

        //4. sink
        dataSet.writeAsCsv(filePath, FileSystem.WriteMode.OVERWRITE).setParallelism(1);

        //5. execute
        env.execute();

    }
}
