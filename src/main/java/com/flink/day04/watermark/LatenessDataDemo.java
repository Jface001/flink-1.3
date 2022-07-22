package com.flink.day04.watermark;


import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * flink默认情况下会将迟到的数据丢弃，但是对于绝大多数的业务中是不允许删除迟到数据的，因此可以使用flink的延迟数据处理机制进行数据的获取并处理
 *
 * 每5s一个窗口，水印：3s，延迟等待：2s
 * 测试数据：
 * hadoop,1626936202000  -> 2021-07-22 14:43:22
 * hadoop,1626936207000  -> 因为设置了水印，所以不会触发窗口计算
 * hadoop,1626936202000  -> 还是第一个窗口的数据
 * hadoop,1626936203000
 * hadoop,1626936208000  -> 触发了窗口的计算，减去水印时间依然满足窗口的endtime
 * hadoop,1626936202000  -> 已经触发过计算的窗口再次有新数据到达，依然再次触发窗口计算（已经触发过计算的窗口，属于这个窗口的数据每次达到都会再次触发）（数据重复计算）
 * hadoop,1626936203000
 * hadoop,1626936209000
 * hadoop,1626936202000
 * hadoop,1626936210000  -> 满足了窗口销毁的条件
 * hadoop,1626936202000  -> 打印迟到数据
 * hadoop,1626936215000
 */
public class LatenessDataDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<String> lines = env.socketTextStream("node01", 8888);

        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = lines.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String value) throws Exception {
                String[] data = value.split(",");
                return new Tuple2<String, Long>(data[0], Long.parseLong(data[1]));
            }
        });

        //应用水印操作
        SingleOutputStreamOperator<Tuple2<String, Long>> watermarkStream = wordAndOne.assignTimestampsAndWatermarks(
                WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                return element.f1;
                            }
                        })
        );

        WindowedStream<Tuple2<String, Long>, String, TimeWindow> windowStream = watermarkStream.keyBy(t -> t.f0).window(
                TumblingEventTimeWindows.of(Time.seconds(5))
        ).allowedLateness(Time.seconds(2));//todo 设置允许延迟的时间是通过allowedLateness(lateness: Time)设置

        OutputTag<Tuple2<String, Long>> outputTag = new OutputTag<>("side output", TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {}));
        //获取到延迟到达的数据
        //todo 保存延迟数据则是通过sideOutputLateData(outputTag: OutputTag[T])保存
        WindowedStream<Tuple2<String, Long>, String, TimeWindow> sideOutputLateData = windowStream.sideOutputLateData(outputTag);

        SingleOutputStreamOperator<Tuple2<String, Long>> result = sideOutputLateData.apply(new WindowFunction<Tuple2<String, Long>, Tuple2<String, Long>, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<Tuple2<String, Long>> out) throws Exception {
                String key = null;
                Long counter = 0L;
                for (Tuple2<String, Long> ele : input) {
                    key = ele.f0;
                    counter += 1;
                }
                out.collect(Tuple2.of(key, counter));
            }
        });

        result.print("正常到达的数据>>>");

        //todo 获取延迟数据是通过DataStream.getSideOutput(tag: OutputTag[X])获取
        DataStream<Tuple2<String, Long>> sideOutput = result.getSideOutput(outputTag);
        sideOutput.printToErr("延迟到达的数据>>>");

        env.execute();
    }
}
