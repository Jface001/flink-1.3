package com.flink.day02.transformation;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @author jface
 * @create 2022/2/12 20:58
 * @desc
 */
public class ConnectDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<Long> ds1 = env.addSource(new MyNoParallelSource());
        DataStreamSource<Long> ds2 = env.addSource(new MyNoParallelSource());
        //3. connect
        SingleOutputStreamOperator<String> strDs2 = ds2.map(new MapFunction<Long, String>() {
            @Override
            public String map(Long value) throws Exception {
                return "str_" + value;
            }
        });

        ConnectedStreams<Long, String> connectedStreams = ds1.connect(strDs2);
        SingleOutputStreamOperator<Object> result = connectedStreams.map(new CoMapFunction<Long, String, Object>() {
            @Override
            public Object map1(Long value) throws Exception {
                System.out.println("第一个数据集的数据：" + value);
                return value;
            }

            @Override
            public Object map2(String value) throws Exception {
                System.out.println("第二个数据集的数据：" + value);
                return value;
            }
        });

        //4. sink
        result.print();
        //5. execute
        env.execute("connect demo");

    }


    // 自定义数据源
    private static class MyNoParallelSource implements SourceFunction<Long> {
        // 定义变量，是否循环生成变量
        private boolean isRunning = true;
        // 定义变量
        private Long count = 0L;


        @Override
        public void run(SourceContext<Long> ctx) throws Exception {
            while (isRunning) {
                count++;
                // 返回数据
                ctx.collect(count);

                // 每秒生成一个数据
                TimeUnit.SECONDS.sleep(1);
            }

        }

        @Override
        public void cancel() {
            isRunning = false;

        }
    }


}
