package com.flink.day03.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author jface
 * @create 2022/2/26 22:41
 * @desc 案例: 自定义一个Source, 每隔随机的秒(1~10)之间产生1条数据
 * 数据是key value, key: hadoop spark flink 其中一个, value: 是随机的数字
 * 需求1：定义一个会话时间窗口, 5秒gap, 统计全量数据之和
 * 需求2: 定义一个会话时间窗口, 5秒gap, 统计按照key分组后的每个组数据内的数字和
 */
public class TimeSessionWindowDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataStreamSource<Tuple2<String, Integer>> streamSource = env.addSource(new GeneraterRandomNumSource());
        streamSource.printToErr("streamSource>>>");

        //3. transform
        //all data
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumOfAll = streamSource
                .windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1);

        //each group
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumEasyKey = streamSource
                .keyBy(t -> t.f0)
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)))
                .sum(1);

        //4. sink
        sumOfAll.printToErr("sumOfAll>>>");
        sumEasyKey.printToErr("sumEasyKey>>>");


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
