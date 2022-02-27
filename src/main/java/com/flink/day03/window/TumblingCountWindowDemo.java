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
 * @create 2022/2/26 22:46
 * @desc
 * 每隔5条统计数据, 分别统计
 * 1. 全量数字之和
 * 2. 分组后每个key对应的数字之和
 */
public class TumblingCountWindowDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataStreamSource<Tuple2<String, Integer>> streamSource = env.addSource(new GeneraterRandomNumSource());
        streamSource.printToErr("streamSource>>>");

        //3. transform
        // all data
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumOfAll = streamSource
                .countWindowAll(5)
                .sum(1);

        // keyBy data
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumEasyKey = streamSource
                .keyBy(t -> t.f0)
                .countWindow(5)
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
