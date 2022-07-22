package com.flink.day05.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 演示flink的重启策略
 * flink的重启策略是在配置了checkpoint的前提下，不停的重启，如果不配置checkpoint不能使用重启策略，作业直接停止
 * flink提供了三种重启策略的方式
 * 固定延迟重启策略：
 *      设置失败重启的次数，以及两次重启的时间间隔，如L失败重启次数为3，每次间隔5s重试，连续失败三次作业停止
 * 失败率重启策略：
 *      给定一个时间，如果这个时间内设置了n次失败重启，一旦超过n此则作业停止，如：5分钟内失败了五次，每次时间重试间隔是10s，则任务失败
 *  无重启策略：
 *      表示运行失败以后，立刻停止作业的运行
 */
public class FixedDelayRestartStrategyDemo {
    public static void main(String[] args) throws Exception {
        //todo 1）初始化flink流处理的运行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //todo 2）开启checkpoint
        //每隔5s周期性的生成barrier（栅栏），默认情况下没有开启checkpoint
        env.enableCheckpointing(5000L);
        //设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(2000L);
        //同一个时间只能有一个栅栏在运行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //设置checkpoint的执行模式。仅执行一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //设置checkpoint最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(1000L);
        //env.getCheckpointConfig().setFailOnCheckpointingErrors(false);
        //指定checkpoint的存储位置
        if(args.length< 1){
            //env.setStateBackend(new FsStateBackend("file:///D:\\checkpoint"));
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage("file:///D:\\checkpoint");
        }else{
            //env.setStateBackend(new FsStateBackend(args[0]));
            env.setStateBackend(new HashMapStateBackend());
            env.getCheckpointConfig().setCheckpointStorage(args[0]);
        }

        //取消作业的时候，上一次成功的checkpoint结果，被删除了，意味着不能将上次执行累加的结果无法恢复，因此希望在取消作业的时候，不要删除已经checkpoint成功的历史结果数据
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION: 当作业被取消的时候，保留外部的checkpoint，注意在此情况下，必须要手动的清除checkpoint
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 当作业被取消的时候，删除外部的checkpoint，只有当作业执行失败时，检查点状态才可用
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //todo 3）配置重启策略
        //固定延迟重启策略。程序出现异常的时候，重启三次，每次延迟5s重启，超过三次，则程序退出
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(5)));

        //todo 4）接入数据源，读取文件获取数据
        DataStreamSource<String> lines = env.socketTextStream("node01", 7777);

        //todo 5）数据处理
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> out) throws Exception {
                if(line.startsWith("laowang")){
                    System.out.println(1/0);
                    throw new RuntimeException("老王出现，程序宕机！");
                }
                String[] words = line.split(" ");
                //返回数据
                for (String word : words) {
                    out.collect(word);
                }
            }
        });

        //  3.2：对拆分后的单词进行记一次数
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        //  3.3：使用分组算子对key进行分组
        //wordAndOne.keyBy(0);
//        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
//            @Override
//            public String getKey(Tuple2<String, Integer> value) throws Exception {
//                return value.f0;
//            }
//        });
        KeyedStream<Tuple2<String, Integer>, String> grouped = wordAndOne.keyBy(t -> t.f0);

        //  3.4：对分组后的key进行聚合操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> sumed = grouped.sum(1);

        //todo 4）构建sink，输出结果
        sumed.print();

        //todo 5）启动运行
        env.execute();

    }
}
