package com.flink.day04.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * 每一个事件时间都会产生一个watermark
 */
public class WaterMark_Punctuated {
    public static void main(String[] args) throws Exception {
        //todo 1）创建flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 2）为了方便测试，将并行度设置为1
        env.setParallelism(1);

        //设置watermark的生成周期,默认是200ms
        env.getConfig().setAutoWatermarkInterval(2000L);

        //todo 3）接入数据源
        SingleOutputStreamOperator<WaterSensor> lines = env.socketTextStream("node01", 8888)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.parseInt(data[2]));
                    }
                });

        //todo 4）应用水印操作
        SingleOutputStreamOperator<WaterSensor> waterMarkStream = lines.assignTimestampsAndWatermarks(WatermarkStrategy.forGenerator(
                new WatermarkGeneratorSupplier<WaterSensor>() {
                    @Override
                    public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                        return new MyWatermarkGenerator();
                    }
                }
        ).withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs(); //指定事件时间
            }
        }));

        waterMarkStream.print("数据>>>");
        //todo 4）应用窗口操作
        WindowedStream<WaterSensor, String, TimeWindow> sensorWS = waterMarkStream.keyBy(sensor -> sensor.getId())
                .window(TumblingEventTimeWindows.of(Time.seconds(10)));

        //todo 5）定义窗口函数
        SingleOutputStreamOperator<String> result = sensorWS.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                out.collect("key：" + s + "\n" +
                        "数据为：" + elements + "\n" +
                        "数据条数：" + elements.spliterator().estimateSize() + "\n" +
                        "窗口时间为：" + context.window().getStart() + " -> " + context.window().getEnd());
            }
        });

        //todo 6）输出测试
        result.print();

        //todo 启动运行
        env.execute();
    }

    /**
     * 自定义水印操作
     * @param <T>
     */
    public static class MyWatermarkGenerator<T> implements WatermarkGenerator<T>{
        //定义变量，存储当前窗口中最大的时间戳
        private long maxTimestamp = -1L;
        /**
         * 每条数据都会调用该方法
         * @param event
         * @param eventTimestamp
         * @param output
         */
        @Override
        public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {
            System.out.println("on Event...");
            //发射watermark
            output.emitWatermark(new Watermark(maxTimestamp -1L));
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        }

        /***
         * 周期性的执行，默认是200ms调用一次
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            System.out.println("on Periodic..."+System.currentTimeMillis());
        }
    }

    /**
     * 水位传感器，用来接受水位数据
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class  WaterSensor{
        private String id;  //传感器id
        private long ts;    //时间
        private Integer vc; //水位
    }
}

