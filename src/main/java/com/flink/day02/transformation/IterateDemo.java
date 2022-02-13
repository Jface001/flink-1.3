package com.flink.day02.transformation;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.IterativeStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/13 13:29
 * @desc Iterate transformation
 */
public class IterateDemo {
    public static void main(String[] args) throws Exception {
        //1. Stream env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
        //.3 transformation, iterate
        SingleOutputStreamOperator<Long> nums = lines.map(Long::parseLong);

        // iterate
        IterativeStream<Long> iterate = nums.iterate();

        SingleOutputStreamOperator<Long> iterateBody = iterate.map(new MapFunction<Long, Long>() {
            @Override
            public Long map(Long value) throws Exception {
                System.out.println("iterate: " + value);
                return value -= 2;
            }
        });
        // value > b
        SingleOutputStreamOperator<Long> feeback = iterateBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 0;
            }
        });

        // iterate condition
        iterate.closeWith(feeback);

        //4. sink
        SingleOutputStreamOperator<Long> output = iterateBody.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value <= 0;
            }
        });

        output.print("output value>>>");

        //5. execute
        env.execute("IterateDemo");
    }

    }

