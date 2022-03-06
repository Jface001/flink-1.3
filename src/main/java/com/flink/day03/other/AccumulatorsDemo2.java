package com.flink.day03.other;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.IntCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.concurrent.ExecutionException;

/**
 * @author jface
 * @create 2022/3/6 13:30
 * @desc 演示使用累加器
 */
public class AccumulatorsDemo2 {
    public static void main(String[] args) throws Exception {
        //1. env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataSource<Long> source = env.generateSequence(1, 10);


        //3. transform
        // use accumulator
        MapOperator<Long, Long> mapped = source.map(new RichMapFunction<Long, Long>() {
            // declare accumulator
            IntCounter counter = new IntCounter();

            // register accumulator
            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);
                getRuntimeContext().addAccumulator("sum", counter);

            }

            @Override
            public Long map(Long value) throws Exception {
                // use accumulator
                counter.add(1);
                System.out.println("Thread id: " + getRuntimeContext().getIndexOfThisSubtask() + ",counter: " + counter.getLocalValue());
                return value;
            }
        });


        //4. sink
        mapped.writeAsText("data/output/accumulators", FileSystem.WriteMode.OVERWRITE);

        //5. execute
        JobExecutionResult jobExecutionResult = env.execute();

        // get accumulator value
        int sum = jobExecutionResult.getAccumulatorResult("sum");
        System.out.println("Finally Accumulator Result is : " + sum);
    }
}
