package com.flink.day02.transformation;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/13 14:43
 * @desc
 *  minBy value and maxBy value
 */
public class MinByAndMaxByDemo2 {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // mock datas
        //辽宁,沈阳,1000
        //北京,朝阳,8000
        //辽宁,朝阳,1000
        //辽宁,朝阳,1000
        //辽宁,沈阳,2000
        //北京,朝阳,1000
        //辽宁,大连,3000
        //辽宁,铁岭,500

        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        //3. transformation
        SingleOutputStreamOperator<Tuple3<String, String, Double>> wordAndOne = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String lines) throws Exception {
                String[] words = lines.split(",");
                String province = words[0];
                String city = words[1];
                double money = Double.parseDouble(words[2]);
                return Tuple3.of(province, city, money);
            }
        });

        KeyedStream<Tuple3<String, String, Double>, String> keyed = wordAndOne.keyBy(w -> w.f0);

        // minBy and maxBy
        keyed.minBy(2).print("min value>>>");
        keyed.maxBy(2).print("max value>>>");

        //4. sink
        //5. execute
        env.execute();

    }
}
