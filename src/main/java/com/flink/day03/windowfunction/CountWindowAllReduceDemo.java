package com.flink.day03.windowfunction;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * @author jface
 * @create 2022/2/28 22:07
 * @desc ReduceFunction定义了如何把两个输入的元素进行合并来生成相同类型的输出元素的过程，Flink使用ReduceFunction来对窗口中的元素进行增量聚合
 * 需求：不分组，划分窗口
 * 然后调用reduce对窗口内的数据进行聚合
 */
public class CountWindowAllReduceDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);

        //3. transform
        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //window
        //GlobalWindow有几个并行？一个并行，只有一个分区（窗口中只有一个subtask）
        AllWindowedStream<Integer, GlobalWindow> windowed = nums.countWindowAll(5);

        SingleOutputStreamOperator<Integer> result = windowed.reduce(new ReduceFunction<Integer>() {
            @Override
            public Integer reduce(Integer value1, Integer value2) throws Exception {
                return value1 + value2;//增量聚合，不是满足条件再计算的，因此该方式效率更高，更节省资源
            }
        });


        //4. sink
        result.print();

        //5. execute
        env.execute();
    }
}
