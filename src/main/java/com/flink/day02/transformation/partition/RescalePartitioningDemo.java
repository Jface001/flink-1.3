package com.flink.day02.transformation.partition;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author jface
 * @create 2022/2/19 22:00
 * @desc
 */
public class RescalePartitioningDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        //3. transform
        SingleOutputStreamOperator<String> mapped = lines.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                return value + "<--->" + index;
            }
        }).setParallelism(1);

        //rescale
        DataStream<String> global = mapped.rescale();

        //4. sink
        global.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value + "<--->" + index);
            }
        });

        //5. execute
        env.execute();

    }
}
