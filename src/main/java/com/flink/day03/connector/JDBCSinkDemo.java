package com.flink.day03.connector;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/2/22 20:20
 * @desc socket --> jdbc
 */
public class JDBCSinkDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //checkpoint
        env.enableCheckpointing(5000);
        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
        //3. transform
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] arr = value.split(" ");
                for (String word : arr) {
                    out.collect(Tuple2.of(word, 1));
                }
            }
        });

        //keyBy and sum
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = wordAndOne.keyBy(t -> t.f0).sum(1);

        //4. sink
        sumed.print();

        sumed.addSink(JdbcSink.sink(
                "insert into t_wordcount(word,count) values(?,?) on duplicate key update count=?",
                (ps, t) -> {
                    ps.setString(1, t.f0);
                    ps.setInt(2, t.f1);
                    ps.setInt(3, t.f1);
                },
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:mysql://localhost:3306/flink")
                        .withDriverName("com.mysql.jdbc.Driver")
                        .withUsername("root")
                        .withPassword("123456")
                        .build()));


        //5. execute
        env.execute();
    }
}
