package com.flink.day03.windowfunction;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/3/5 19:06
 * @desc 使用apply方法来实现单词统计
 */
public class WindowApplyDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        //3. transform
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {


            @Override
            public void flatMap(String lines, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] words = lines.split(" ");
                for (String word : words) {
                    out.collect(new Tuple2<>(word, 1));
                }

            }
        });

        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> windowStream = wordAndOne
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        SingleOutputStreamOperator<Tuple2<String, Integer>> result = windowStream.apply(new WindowFunction<Tuple2<String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow timeWindow, Iterable<Tuple2<String, Integer>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {
                int sum = 0;
                for (Tuple2<String, Integer> tuple2 : iterable) {
                    sum += tuple2.f1;
                }
                collector.collect(new Tuple2<>(s, sum));
            }
        });

        //4. sink
        result.print();

        //5. execute
        env.execute();
    }
}
