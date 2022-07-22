package com.flink.day04.watermark;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.*;

/**
 * 多并行度下的水印操作演示
 *
 * 测试数据：
 * 并行度设置为2测试：
 * hadoop,1626934802000 ->2021-07-22 14:20:02
 * hadoop,1626934805000 ->2021-07-22 14:20:05
 * hadoop,1626934806000 ->2021-07-22 14:20:06
 *
 * 结论：两个并行度都满足了窗口触发的条件，则窗口会被触发计算，同时以两个并行度中小的水印为准（对齐）
 * 键值：hadoop，线程号：76，事件时间：【2021-07-22 14:20:02.000】
 * 键值：hadoop，线程号：77，事件时间：【2021-07-22 14:20:05.000】
 * 键值：hadoop，线程号：76，事件时间：【2021-07-22 14:20:06.000】
 * 触发窗口计算结果>>>:2>
 *  键值：【(hadoop)】
 *      触发窗口数据的个数：【1】
 *      触发窗口的数据：2021-07-22 14:20:02.000
 *      窗口计算的开始时间和结束时间：2021-07-22 14:20:00.000----->2021-07-22 14:20:05.000
 *
 *
 * 触发窗口计算的前提：
 * 有几个线程那么需要将这些线程全部的满足窗口触发的条件才会触发窗口计算（窗内必须有数据）
 */
public class Watermark_Parallelism {
    //定义打印数据的日期格式
    final private static SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

    public static void main(String[] args) throws Exception {
        /**
         * 实现步骤：
         * 1）初始化flink流处理的运行环境
         * 2）设置数据处理的时间为事件时间（EventTime）
         * 3）为了方便观察数据，将并行度设置为1
         * 4）构建数据源
         * 5）对数据进行操作
         * 5.1：切割字符串（验证数据格式）
         * 5.2：为当前数据流添加水印
         * 5.3：根据key分流
         * 5.4：对分流后的数据进行时间窗口划分（滚动窗口）
         * 6）打印测试
         * 7）启动作业
         */
        //TODO　1）初始化flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO　2）设置数据处理的时间为事件时间（EventTime）
        //env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //TODO　3）为了方便观察数据，将并行度设置为2
        env.setParallelism(8);

        //TODO　4）构建数据源
        DataStreamSource<String> socketTextStream = env.socketTextStream("node01", 8888);

        //TODO　5）对数据进行操作
        //5.1：切割字符串（验证数据格式）
        SingleOutputStreamOperator<Tuple2<String, Long>> tupleDataStream = socketTextStream.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String line) throws Exception {
                try {
                    String[] array = line.split(",");
                    return Tuple2.of(array[0], Long.parseLong(array[1]));
                } catch (NumberFormatException e) {
                    System.out.println("输入的数据格式不正确：" + line);
                    return Tuple2.of("null", 0L);
                }
            }
        }).filter(new FilterFunction<Tuple2<String, Long>>() {
            @Override
            public boolean filter(Tuple2<String, Long> tuple) throws Exception {
                if (!tuple.f0.equals("null") && tuple.f1 != 0L) return true;
                return false;
            }
        });

        //5.2：为当前数据流添加水印
        /**
         * AssignerWithPeriodicWatermarks：周期性水印，是用的最多的水印，周期性分配水位线，指定系统以一个固定的周期设定的时间间隔发出水位线
         *                                 在设置时间为事件时间时，默认设置这个时间间隔为200ms（每隔200毫秒生成一个水位线），这种方式水位线生成比较稳定
         * AssignerWithPunctuatedWatermarks：标记性水印，也称之为定点水位线，很少使用，通过对数据流中某些特定的标记事件触发新水位线的生成，这种方式下窗口的触发
         *                                  与时间无关，但是与数据有关，决定窗口触发的前提是何时收到某种标记的事件。
         */
        SingleOutputStreamOperator<Tuple2<String, Long>> waterMarkDataStream = tupleDataStream.assignTimestampsAndWatermarks(
                //TODO 自定义watermark生成器
                WatermarkStrategy
                        .<Tuple2<String, Long>>forGenerator(
                                new WatermarkGeneratorSupplier<Tuple2<String, Long>>() {
                                    @Override
                                    public WatermarkGenerator<Tuple2<String, Long>> createWatermarkGenerator(Context context) {
                                        return new MyWatermarkGenerator<>();
                                    }
                                })
                        .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Long>>() {
                            @Override
                            public long extractTimestamp(Tuple2<String, Long> element, long recordTimestamp) {
                                //获取数据中携带的eventTime
                                Long timestamp = element.f1;

                                //定义打印输出的字符串
                                System.out.println("键值：" + element.f0 + "，线程号：" + Thread.currentThread().getId() + "，" +
                                        "事件时间：【" + sdf.format(timestamp) + "】");
                                return timestamp;
                            }
                        })
        );

        //5.3：根据key分流
        //5.4：对分流后的数据进行时间窗口划分（滚动窗口）
        SingleOutputStreamOperator<String> result = waterMarkDataStream
                .keyBy(0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))//每隔5秒钟划分一次窗口
                .apply(new WindowFunction<Tuple2<String, Long>, String, Tuple, TimeWindow>() {
                    /**
                     * 对window窗口内的数据进行排序，保证数据的顺序输出
                     *
                     * @param tuple
                     * @param window
                     * @param input
                     * @param out
                     * @throws Exception
                     */
                    @Override
                    public void apply(Tuple tuple, TimeWindow window, Iterable<Tuple2<String, Long>> input, Collector<String> out)
                            throws Exception {
                        //定义该窗口所有时间字段的集合对象
                        List<Long> timeArr = new ArrayList<>();
                        //获取到窗口内所有的数据
                        Iterator<Tuple2<String, Long>> iterator = input.iterator();
                        while (iterator.hasNext()) {
                            Tuple2<String, Long> tuple2 = iterator.next();
                            timeArr.add(tuple2.f1);
                        }

                        //对保存到集合列表的时间进行排序
                        Collections.sort(timeArr);

                        //打印输出该窗口触发的所有数据
                        String outputData = "" +
                                "\n 键值：【" + tuple + "】" +
                                "\n     触发窗口数据的个数：【" + timeArr.size() + "】" +
                                "\n     触发窗口的数据：" + sdf.format(new Date(timeArr.get(timeArr.size() - 1))) +
                                "\n     窗口计算的开始时间和结束时间：" + sdf.format(new Date(window.getStart())) + "----->" +
                                sdf.format(new Date(window.getEnd()));

                        out.collect(outputData);
                    }
                });

        //TODO　6）打印测试
        result.printToErr("触发窗口计算结果>>>");

        //TODO　7）启动作业
        env.execute();
    }

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
            //System.out.println("on Event...");
            maxTimestamp = Math.max(maxTimestamp, eventTimestamp);
        }

        /***
         * 周期性的执行，默认是200ms调用一次
         * @param output
         */
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            //System.out.println("on Periodic..."+System.currentTimeMillis());
            //发射watermark
            output.emitWatermark(new Watermark(maxTimestamp -1L));
        }
    }
}
