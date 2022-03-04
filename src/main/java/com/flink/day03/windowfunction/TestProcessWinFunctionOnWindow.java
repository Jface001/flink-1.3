package com.flink.day03.windowfunction;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

/**
 * @author jface
 * @create 2022/3/4 16:44
 * @desc processWindowFunction
 */
public class TestProcessWinFunctionOnWindow {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<Tuple3<String, String, Long>> inputSource = env.fromElements(ENGLISH);

        //3. transform
        SingleOutputStreamOperator<Double> result = inputSource.keyBy(t -> t.f0).countWindow(2).process(new MyProcessWindowFunction());

        //4. sink
        result.print();


        //5. execute
        env.execute();

    }

    public static final Tuple3[] ENGLISH = new Tuple3[]{Tuple3.of("class1", "张三", 100L), Tuple3.of("class1", "李四", 40L), Tuple3.of("class1", "王五", 60L), Tuple3.of("class2", "赵六", 20L), Tuple3.of("class2", "小七", 30L), Tuple3.of("class2", "小八", 50L),};


    public static class MyProcessWindowFunction extends ProcessWindowFunction<Tuple3<String, String, Long>, Double, String, GlobalWindow> {

        @Override
        public void process(String key, Context context, Iterable<Tuple3<String, String, Long>> elements, Collector<Double> out) throws Exception {

            long sum = 0;
            long count = 0;
            for (Tuple3<String, String, Long> in : elements) {
                sum += in.f2;
                count++;
            }
            out.collect(sum * 1.0 / count);

        }
    }
}
