package com.flink.day02.sink;


import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * @author jface
 * @create 2022/2/20 17:33
 * @desc
 */
public class PrintSinkDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        Configuration configuration = new Configuration();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

        int parallelism0 = env.getParallelism();
        System.out.println("env parallelism:" + parallelism0);

        //2. source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 9999);
        //3. transform
        int parallelism = lines.getParallelism();
        System.out.println("lines parallelism:" + parallelism);

        //4. sink
        lines.addSink(new MyPrintSink()).name("my-print-sink");

        //5. execute
        env.execute();
    }

    public static class MyPrintSink extends RichSinkFunction<String> {
        private int indexOfThisSubtask;

        @Override
        public void open(Configuration parameters) throws Exception {
            RuntimeContext runtimeContext = getRuntimeContext();
            indexOfThisSubtask = runtimeContext.getIndexOfThisSubtask();
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            System.out.println(indexOfThisSubtask + 1 + ">" + value);
        }
    }
}
