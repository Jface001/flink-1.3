package com.flink.day03.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author jface
 * @create 2022/2/24 21:42
 * @desc 滑动计数窗口案例
 * 每隔5条统计数据, 统计前10行
 * 1. 全量数字之和
 * 2. 分组后每个key对应的数字之和
 * 窗口长度5, 滑动距离10
 */
public class SlidingCountWindowDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataStreamSource<Tuple2<String, Integer>> streamSource = env.addSource(new GeneraterRandomNumSource());
        streamSource.printToErr("mock data>>>");

        //3. transform
        // window 1 未分流的的数据统计数字总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumOfAll = streamSource
                .countWindowAll(10, 5)
                .sum(1);

        // window 2 按 key 分组后的数据统计每个 key 对应的数字总和
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumEasyKey = streamSource
                .keyBy(t -> t.f0)
                .countWindow(10, 5)
                .sum(1);


        //4. sink
        sumOfAll.print("sum of all>>>");
        sumEasyKey.print("sum of easy key>>>");

        //5. execute
        env.execute();
    }

    public static class GeneraterRandomNumSource implements SourceFunction<Tuple2<String, Integer>> {

        private boolean isRunning = true;
        Random random = new Random();
        List<String> keyList = Arrays.asList("hadoop", "spark", "flink");

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

