package com.flink.day02.transformation.partition;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author jface
 * @create 2022/2/16 22:11
 * @desc
 * 使用用户定义的 Partitioner 为每个元素选择目标任务。
 * 编写Flink程序，接收socket的单词数据，并将每个字符串写入到指定的分区中。
 */
public class CustomPartitioningDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        //3. transform
        SingleOutputStreamOperator<Tuple2<String, Integer>> mapped = lines.map(new RichMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return Tuple2.of(value, indexOfThisSubtask);
            }
        });

        // custom partitioning
        DataStream<Tuple2<String, Integer>> partitioned = mapped.partitionCustom(new Partitioner<String>() {
            @Override
            public int partition(String key, int numPartitions) {
                //System.out.println("key: " + key  + " ,下游task的并行度：" + numPartitions);
                int res = 0;
                if ("spark".equals(key)) {
                    res = 1;
                } else if ("flink".equals(key)) {
                    res = 2;
                } else if ("hadoop".equals(key)) {
                    res = 3;
                }
                return res;

            }
        }, t -> t.f0);

        //4. sink
        partitioned.addSink(new RichSinkFunction<Tuple2<String, Integer>>() {
            @Override
            public void invoke(Tuple2<String, Integer> value, Context context) throws Exception {
                int index = getRuntimeContext().getIndexOfThisSubtask();
                System.out.println(value.f0+", 上游"+ value.f1+" -> 下游"+index);
            }
        });

        //5. execute
        env.execute();
    }
}
