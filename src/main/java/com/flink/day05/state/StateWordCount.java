package com.flink.day05.state;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import scala.Int;
import scala.collection.convert.Wrappers;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.concurrent.TimeUnit;

/**
 * 读取文件数据，每隔20s读取一条，然后将读取到的字符串按照空格拆分计算每个单词出现的次数
 * 设置state存储每个单词的出现次数，state的过期时间设置为1分钟
 */
public class StateWordCount {
    public static void main(String[] args) throws Exception {
        //todo 1）使用flink的工具类读取传入值参数进行解析成ParameterTool对象
        final ParameterTool parameters = ParameterTool.fromArgs(args);

        //todo 2）flink流处理环境的初始化
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);
        
        //todo 3）将传入值解析成的ParameterTool对象注册到作业中去
        env.getConfig().setGlobalJobParameters(parameters);

        //todo 4）开启checkpoint
        env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE);

        //todo 5）接入数据源
        DataStreamSource<String> lines = env.addSource(new SourceFunctionFile());
        lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] data = value.split(" ");
                for (String item : data){
                    out.collect(new Tuple2<>(item, 1));
                }
            }
        }).keyBy(t->t.f0).flatMap(new WordCountFlatMap()).printToErr();

        //todo 运行作业
        env.execute();
    }

    /**
     * 自定义数据源，读取文件的数据
     */
    private static class SourceFunctionFile extends RichSourceFunction<String> {
        //定义是否继续生成数据的标记
        private boolean isRunning = true;
        @Override
        public void run(SourceContext<String> ctx) throws Exception {
            BufferedReader bufferedReader = new BufferedReader(new FileReader("./data/input/wordcount.txt"));
            while (isRunning) {
                String line = bufferedReader.readLine();
                if (StringUtils.isBlank(line)) {
                    continue;
                }
                ctx.collect(line);
                TimeUnit.SECONDS.sleep(10);
            }
        }

        @Override
        public void cancel() {
            isRunning = false;
        }
    }

    /**
     * 自定义聚合操作
     */
    private static class WordCountFlatMap  extends RichFlatMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        //定义状态
        private ValueState<Tuple2<String, Integer>> valueState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //创建ValueStateDescriptor
            ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor("StateWordCount",
                    TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));

            //配置stateTTL
            StateTtlConfig ttlConfig = StateTtlConfig
                    .newBuilder(Time.seconds(5)) //存活时间
                    .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite) //创建或者更新的时候修改过期时间
                    .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)//永不返回过期的数据
                    .cleanupFullSnapshot() //全量快照时进行清理，不可以使用在状态存储后端是rocksdb的场景
                    //.cleanupIncrementally(10, true) //增量数据清理，不可以使用在状态存储后端是rocksdb的场景
                    //.cleanupInRocksdbCompactFilter(1000) //如果checkpoint的状态存储后端是rocksdb的话可以设置这种清理数据的方式
                    .setTtlTimeCharacteristic(StateTtlConfig.TtlTimeCharacteristic.ProcessingTime) //目前只支持ProcessingTime，可以忽略
                    .build();
            //激活stateTTL
            valueStateDescriptor.enableTimeToLive(ttlConfig);

            //实例化valueState对象
            valueState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public void close() throws Exception {
            super.close();
        }

        @Override
        public void flatMap(Tuple2<String, Integer> value, Collector<Tuple2<String, Integer>> out) throws Exception {
            Tuple2<String, Integer> currentState = valueState.value();

            //初始化ValueState值
            if(null == currentState){
                currentState = new Tuple2<>(value.f0, 0);
            }

            //累加单词的次数
            Tuple2<String, Integer> newState = new Tuple2<>(currentState.f0, currentState.f1+value.f1);

            //更新valuestate
            valueState.update(newState);

            //返回累加后的结果
            out.collect(newState);
        }
    }
}
