package com.flink.day02.transformation;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * @author jface
 * @create 2022/2/13 15:09
 * @desc  odd and even split
 */
public class SplitDemo {
    public static void main(String[] args) throws Exception {
        // 1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        // 2. source
        DataStreamSource<Integer> ds = env.fromElements(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        // 3. transform
        OutputTag<Integer> evenTag = new OutputTag<>("even", TypeInformation.of(Integer.class));
        OutputTag<Integer> oddTag = new OutputTag<>("odd", TypeInformation.of(Integer.class));

        SingleOutputStreamOperator<Integer> result = ds.process(new ProcessFunction<Integer, Integer>() {
            @Override
            public void processElement(Integer value, ProcessFunction<Integer, Integer>.Context ctx, Collector<Integer> out) throws Exception {
                if (value % 2 == 0) {
                    ctx.output(evenTag, value);
                } else ctx.output(oddTag, value);
            }
        });

        DataStream<Integer> oddDataStream = result.getSideOutput(oddTag);
        DataStream<Integer> evenDataStream = result.getSideOutput(evenTag);
        // 4. sink
        oddDataStream.print("奇数>>>");
        evenDataStream.print("偶数>>>");

        //5. execute
        env.execute();
    }

    }

