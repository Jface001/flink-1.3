package com.flink.day05.state;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * 广播状态流演示
 * 需求：公司有10个广告位, 其广告的内容(描述和图片)会经常变动（广告到期，更换广告等）
 * 实现：
 * 1）通过socket输入广告id（事件流）
 * 2）关联出来广告的信息打印出来，就是广告发生改变的时候，能够感知到（规则流）
 */
public class BroadcastStateDemo {
    public static void main(String[] args) throws Exception {
        //todo 1）初始化flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 2）设置checkpoint周期性运行
        env.enableCheckpointing(5000L);

        //todo 3）构建数据源
        //构建事件流
        DataStreamSource<String> lines = env.socketTextStream("node01", 7777);
        SingleOutputStreamOperator<Integer> adIdDataStream = lines.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.parseInt(value);
            }
        });

        //构建规则流（广告流）
        DataStreamSource<Map<Integer, Tuple2<String, String>>> adSourceStream = env.addSource(new MySourceForBroadcastFunction());
        adSourceStream.print("最新的广告信息>>>");
        //todo 4）将规则流（广告流）转换成广播流
        //定义广告流的描述器
        MapStateDescriptor<Integer, Tuple2<String, String>> mapStateDescriptor = new MapStateDescriptor<Integer, Tuple2<String, String>>(
                "broadcaststate",
                TypeInformation.of(new TypeHint<Integer>() {}),
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})
        );
        BroadcastStream<Map<Integer, Tuple2<String, String>>> broadcastStream = adSourceStream.broadcast(mapStateDescriptor);

        //todo 5）将两个流合并到一起
        BroadcastConnectedStream<Integer, Map<Integer, Tuple2<String, String>>> connectedStream = adIdDataStream.connect(broadcastStream);

        //todo 6）对关联后的数据进行拉宽操作
        SingleOutputStreamOperator<Tuple2<String, String>> result = connectedStream.process(new MyBroadcastProcessFunction());

        //todo 7）打印测试
        result.printToErr("拉宽后的结果>>>");

        //todo 8）启动作业
        env.execute();
    }

    public static class MyBroadcastProcessFunction extends BroadcastProcessFunction<Integer, Map<Integer, Tuple2<String, String>>, Tuple2<String, String>> {
        //定义state的描述器
        MapStateDescriptor<Integer, Tuple2<String, String>> mapStateDescriptor = new MapStateDescriptor<Integer, Tuple2<String, String>>(
                "broadcaststate",
                TypeInformation.of(new TypeHint<Integer>() {}),
                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})
        );

        /**
         * 这个方法是只读的，用来拉宽操作
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processElement(Integer value, ReadOnlyContext ctx, Collector<Tuple2<String, String>> out) throws Exception {
            //只读操作，意味着只能读取数据，不能修改数据，根据广告id获取广告信息
            ReadOnlyBroadcastState<Integer, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            //根据广告id获取广告信息
            Tuple2<String, String> tuple2 = broadcastState.get(value);
            //判断广告信息是否关联成功
            if(tuple2 != null) out.collect(tuple2);
        }

        /**
         * 可写的，用来更新state的数据
         * @param value
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(Map<Integer, Tuple2<String, String>> value, Context ctx, Collector<Tuple2<String, String>> out) throws Exception {
            //删除历史状态的数据，重新为state进行赋值操作
            BroadcastState<Integer, Tuple2<String, String>> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
            //删除历史状态的数据
            broadcastState.clear();
            //将最新获取到的广告信息进行广播操作
            broadcastState.putAll(value);
        }
    }

    /**
     * 广告流数据
     * 定义一个可以发生改变的规则流，每个广告id，对应的广告内容每隔5s进行更新一次
     */
    private static class MySourceForBroadcastFunction implements SourceFunction<Map<Integer, Tuple2<String, String>>>{
        private final Random random = new Random();
        private final List<Tuple2<String, String>> ads = Arrays.asList(
                Tuple2.of("baidu", "搜索引擎"),
                Tuple2.of("google", "科技大牛"),
                Tuple2.of("aws", "全球领先的云平台"),
                Tuple2.of("aliyun", "全球领先的云平台"),
                Tuple2.of("腾讯", "氪金使我变强"),
                Tuple2.of("阿里巴巴", "电商龙头"),
                Tuple2.of("字节跳动", "靠算法出名"),
                Tuple2.of("美团", "黄色小公司"),
                Tuple2.of("饿了么", "蓝色小公司"),
                Tuple2.of("瑞幸咖啡", "就是好喝")
        );
        private boolean isRun = true;

            @Override
            public void run(SourceContext<Map<Integer, Tuple2<String, String>>> ctx) throws Exception {
                while (isRun) {
                    Map<Integer, Tuple2<String, String>> map = new HashMap<>();
                    int keyCounter = 0;
                    for (int i = 0; i < ads.size(); i++) {
                        keyCounter++;
                        map.put(keyCounter, ads.get(random.nextInt(ads.size())));
                    }
                    ctx.collect(map);

                    TimeUnit.SECONDS.sleep(5L);
                }
            }

            @Override
            public void cancel() {
                this.isRun = false;
            }
        }
    }
