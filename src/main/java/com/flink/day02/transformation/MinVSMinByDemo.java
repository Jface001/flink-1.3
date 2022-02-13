package com.flink.day02.transformation;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/13 14:56
 * @desc
 *  min and minBy
 *  max and maxBy
 *  difference
 * min 会取最小值记录下来, 其它字段按照顺序填充, 最终将最后一个元素的其它字段和最小值组合形成结果返回
 * minBy 会判断最小值, 将第一个出现的最小值所在的那一行的结果保留, 最终返回出去
 * max maxBy同理
 */
public class MinVSMinByDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<Tuple3<Integer, Integer, Integer>> source = env.fromElements(
                Tuple3.of(1, 3, 2),
                Tuple3.of(1, 1, 2),
                Tuple3.of(1, 2, 3),
                Tuple3.of(1, 111, 1),
                Tuple3.of(1, 1, 1),
                Tuple3.of(1, 2, 0),
                Tuple3.of(1, 33, 2)
        );
        //3. transformation
        source.keyBy(t->t.f0).min(2).print("min>>>");
        source.keyBy(t->t.f0).minBy(2).print("min>>>");

        //4. sink
        //5. execute
        env.execute();
    }
}
