package com.flink.day04.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 使用固定延迟水印，解决数据乱序的场景（大多数情况都是乱序的数据，使用比较多）
 * 需求：从socket接受数据，进行转换操作，然后应用窗口每隔5秒生成一个窗口，使用水印时间触发窗口计算
 *
 * 使用水印的前提：
 * 1：数据必须要携带事件时间
 * 2：指定事件时间作为数据处理的时间
 * 3：指定并行度为1
 * 4：使用之前版本的api的时候，需要增加时间类型的代码
 *
 * 测试数据：
 * sensor_1,1547718199,35       -》2019-01-17 17:43:19
 * sensor_6,1547718201,15       -》2019-01-17 17:43:21
 * sensor_6,1547718205,15       -》2019-01-17 17:43:25
 * sensor_6,1547718210,15       -》2019-01-17 17:43:30
 *
 * todo 固定延迟触发，根据事件时间-最大乱序时间-1得到水印，根据水印时间作为触发窗口的条件
 * 触发窗口计算的两个条件：
 * 1：时间达到窗口的endtime
 * 2：窗口中存在数据
 */
public class OutOfOrdernessWaterMark {
    public static void main(String[] args) throws Exception {
        //todo 1）创建flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        //todo 2）接入数据源
        SingleOutputStreamOperator<WaterSensor> lines = env.socketTextStream("node01", 8888)
                .map(new MapFunction<String, WaterSensor>() {
                    @Override
                    public WaterSensor map(String value) throws Exception {
                        String[] data = value.split(",");
                        return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.parseInt(data[2]));
                    }
                });

        //todo 3）添加水印处理
        //flink1.12之前版本的api编写（单调递增水印本质上还是周期性水印）
//        SingleOutputStreamOperator<WaterSensor> waterMarkStream = lines.assignTimestampsAndWatermarks(
//                new BoundedOutOfOrdernessTimestampExtractor<WaterSensor>(Time.seconds(3)){
//                    @Override
//                    public long extractTimestamp(WaterSensor element) {
//                        return element.getTs()*1000L;
//                    }
//                });
        //flink1.12以后的版本api
        SingleOutputStreamOperator<WaterSensor> waterMarkStream = lines.assignTimestampsAndWatermarks(
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3)).withTimestampAssigner(
                        new SerializableTimestampAssigner<WaterSensor>() {
            @Override
            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                return element.getTs() * 1000L;
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

