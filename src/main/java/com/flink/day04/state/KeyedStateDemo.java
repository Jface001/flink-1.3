package com.flink.day04.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.lang.reflect.Type;

/**
 * @author jface
 * @create 2022/3/11 23:37
 * @desc 演示keyedState的使用
 * state就是状态，是flink中所有算子都是有状态的计算，换句话说flink所有的算子都可以保存中间计算结果，将计算结果保存到state中，
 * 然后通过checkpoint持久化存储到hdfs中
 * 因此state可以认为是flink自带的数据库，是基于内存的数据库，state支持多种类型的数据结构存储，以及可以定义state的名称等操作
 * <p>
 * state根据不同的算子类型，可以划分出来两类state：
 * keyedState：
 * 应用到在KeyBy算子之上的State
 * ValueState-》ValueStateDescriptor创建而来
 * ListState-》ListStateDescriptor创建而来
 * MapState-》MapStateDescriptor创建而来
 * ReducingState-》ReducingStateDescriptor创建而来
 * OperatorState：
 * 应用到非KeyBy算子之上的State，与并行度的数量有关，有多少个并行度就有多少个OperatorState
 * ListState-》ListStateDescriptor创建而来
 * <p>
 * 使用KeyedState进行单词统计
 * 以前直接使用现成的算子既可以实现数据存储State，但是通过自定义实现State可以深入了解数据的存储结果
 */
public class KeyedStateDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // checkPoint
        env.enableCheckpointing(5000);

        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        //3. transform
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAneNum = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String lines) throws Exception {
                String[] data = lines.split(" ");
                return new Tuple2<>(data[0], Integer.parseInt(data[1]));

            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyByDataStream = wordAneNum.keyBy(t -> t.f0);

        // sum by state
        SingleOutputStreamOperator<Tuple2<String, Integer>> reduceState = keyByDataStream.reduce(new RichReduceFunction<Tuple2<String, Integer>>() {
            private ValueState<Tuple2<String, Integer>> valueState = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                ValueState<Tuple2<String, Integer>> valueState = getRuntimeContext().getState(new ValueStateDescriptor<Tuple2<String, Integer>>(
                        "reduceState",
                        TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {

                        })
                ));
            }

            @Override
            public void close() throws Exception {
                super.close();
                // get state
                Tuple2<String, Integer> value = valueState.value();
                System.out.println("======");
                System.out.println(value);
            }

            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                // state history
                Tuple2<String, Integer> result = valueState.value();
                if (result == null) {
                    result = Tuple2.of(value1.f0, value1.f1);
                }
                Tuple2<String, Integer> resultSum = Tuple2.of(result.f0, result.f1 + value2.f1);

                // update state value
                valueState.update(resultSum);
                return resultSum;

            }
        });

        //4. sink
        reduceState.print();

        //5. execute
        env.execute();
    }
}
