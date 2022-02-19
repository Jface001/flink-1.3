package com.flink.day02.transformation.partition;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.operators.PartitionOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author jface
 * @create 2022/2/19 21:52
 * @desc 通过循环的方式依次发送到下游的task
 */
public class RebalanceDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataSource<Long> dataSource = env.generateSequence(0, 100);

        //3. transform
        FilterOperator<Long> filtered = dataSource.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 8;
            }
        });

        // rebalance
        PartitionOperator<Long> rebalance = filtered.rebalance();
        MapOperator<Long, Tuple2<Long, Integer>> longTuple2MapOperator = rebalance.map(new RichMapFunction<Long, Tuple2<Long, Integer>>() {
            @Override
            public Tuple2<Long, Integer> map(Long value) throws Exception {
                return Tuple2.of(value, getRuntimeContext().getIndexOfThisSubtask());
            }
        });

        //4. sink
        longTuple2MapOperator.print();

        //5. execute

    }
}
