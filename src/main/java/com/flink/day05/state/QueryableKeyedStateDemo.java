package com.flink.day05.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.QueryableStateOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.util.Collector;

public class QueryableKeyedStateDemo {
    public static void main(String[] args) throws Exception {
        //设置参数
        //todo 1）使用flink的工具类读取传入值参数进行解析成ParameterTool对象
        final ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String hostName = parameterTool.get("host", "node01");
        Integer port = parameterTool.getInt("port", 7777);
        Integer parallelism = parameterTool.getInt("parallelism", 1);

        //配置环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        //开启客户端代理服务
        configuration.setBoolean(QueryableStateOptions.ENABLE_QUERYABLE_STATE_PROXY_SERVER, true);

        //todo 2）flink流处理环境的初始化
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);

        //todo 3）设置并行度
        env.setParallelism(parallelism);

        //todo 开启checkpoint
        //env.enableCheckpointing(5000);

        //todo 4）接入数据源
        DataStreamSource<String> lines = env.socketTextStream(hostName, port);
        lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                String[] data = value.split(",");
                out.collect(new Tuple2<>(data[0], Integer.parseInt(data[1])));
            }
        }).keyBy(t -> t.f0).map(new MyQueryStateMapFunction()).printToErr();

        env.execute();

    }

    private static class MyQueryStateMapFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {
        //定义状态
        private ValueState<Integer> valueState = null;

        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            //创建ValueStateDescriptor
            ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor(
                    "StateWordCount",
                   Integer.class
            );

            valueStateDescriptor.setQueryable("lastMaxPV");//设置状态可以查询，并指定状态的查询名称
            //实例化valueState对象
            valueState = getRuntimeContext().getState(valueStateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> value) throws Exception {
            Integer currCounter = value.f1;//获取当前输入的单词的次数
            Integer counts = valueState.value();//获取历史的累加值
            if(counts == null)//历史累加值为空
            {
                counts = 0;
            }
            int total = counts + currCounter;//将历史的和最新的进行累加
            valueState.update(total);//更新状态
            value.f1 = total; //将累加后的次数进行重置
            return value; //返回计算好的元组对象
        }
    }
}
