package com.flink.day03.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.api.common.time.Time.seconds;

/**
 * @author jface
 * @create 2022/2/26 22:46
 * @desc 滚动时间窗口
 * 案例：
 * 自定义一个Source, 每隔1秒产生一个的k,v  k是hadoop spark flink 其中某一个, v是随机数字
 * 对数据加窗口, 窗口1对未分流的数据统计数字总和
 * 窗口2对按key分组后的数据统计每个key对应的数字总和
 */
public class TumblingTimeWindowDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataStreamSource<Tuple2<String, Integer>> streamSource = env.addSource(new GeneraterRandomNumSource());
        streamSource.printToErr("streamSource>>>");

        //3. transform
        // all data
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumOfAll = streamSource
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);

        // keyBy data
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumEasyKey = streamSource
                .keyBy(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(1);


        //4. sink
        sumOfAll.print("all data>>>");
        sumEasyKey.print("keyBy data>>>");


        //5. execute
        env.execute();
    }

    /**
     * mock K-V data
     */
    private static class GeneraterRandomNumSource implements SourceFunction<Tuple2<String, Integer>> {
        private Boolean isRunning = true;
        private final Random random = new Random();
        private final List<String> keyList = Arrays.asList("hadoop", "spark", "flink");


        @Override
        public void run(SourceContext<Tuple2<String, Integer>> ctx) throws Exception {
            while (isRunning) {
                String key = keyList.get(random.nextInt(3));
                ctx.collect(Tuple2.of(key, random.nextInt(100)));

                TimeUnit.SECONDS.sleep(1);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;

        }
    }
}
