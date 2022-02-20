package com.flink.day02.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/20 17:24
 * @desc
 * local collection sink
 */
public class CollectionDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataStreamSource<Tuple2<Integer, String>> dataSource = env.fromElements(
                Tuple2.of(1, "zhangsan"),
                Tuple2.of(2, "lisi"),
                Tuple2.of(3, "wangwu"),
                Tuple2.of(4, "zhaoliu")
        );

        //3. transform
        //4. sink
        dataSource.print();
        dataSource.printToErr();
        //注意:
        //Parallelism=1为文件
        //Parallelism>1为文件夹

        //5. execute
        env.execute();

    }
}
