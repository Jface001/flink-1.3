package com.flink.day03.other;


import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.core.fs.FileSystem;

/**
 * @author jface
 * @create 2022/3/6 13:09
 * @desc 演示未使用累加器
 */
public class AccumulatorsDemo1 {
    public static void main(String[] args) throws Exception {
        //1. env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataSource<Long> source = env.generateSequence(1, 10).setParallelism(16);

        //3. transform
        MapOperator<Long, Long> mapped = source.map(new RichMapFunction<Long, Long>() {
            int counter = 0;
            @Override
            public Long map(Long value) throws Exception {
                counter += 1;
                System.out.println("Thread id: " + getRuntimeContext().getIndexOfThisSubtask() + ",counter: " + counter);
                return value;
            }
        });

        //4. sink
        mapped.writeAsText("data/output/accumulators", FileSystem.WriteMode.OVERWRITE);
        //5. execute
        env.execute();
    }
}
