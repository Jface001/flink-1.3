package com.flink.day03.windowfunction;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.FunctionParameter;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.List;

/**
 * @author jface
 * @create 2022/3/2 20:49
 * @desc AggregateFunction 比 ReduceFunction 更加的通用，它有三个参数:输入类型(IN)、累加器类型(ACC)和输出类型(OUT)。
 * 输入类型是输入流中的元素类型
 * ggregateFunction有一个add方法，可以将一个输入元素添加到一个累加器中
 * 该接口还具有创建初始累加器(createAccumulator方法)
 * 将两个累加器合并到一个累加器(merge方法)
 * 以及从累加器中提取输出(类型为OUT)的方法
 */
public class TestAggFunctionOnWindowDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2. source
        DataStreamSource<Tuple3<String, String, Long>> inputSource = env.fromElements(ENGLISH);

        //3. transform
        //GlobalWindow有几个并行？一个并行，只有一个分区（窗口中只有一个subtask）
        SingleOutputStreamOperator<Double> result = inputSource
                .keyBy(t -> t.f0)
                .countWindow(3)
                .aggregate(new AvgAggregate());

        //4. sink
        result.print();

        //5. execute
        env.execute();
    }

    public static final Tuple3[] ENGLISH = new Tuple3[]{
            Tuple3.of("class1", "张三", 100L),
            Tuple3.of("class1", "李四", 40L),
            Tuple3.of("class1", "王五", 60L),
            Tuple3.of("class2", "赵六", 20L),
            Tuple3.of("class2", "小七", 30L),
            Tuple3.of("class2", "小八", 50L),
    };

    //AggregateFunction 比 ReduceFunction 更加的通用，它有三个参数:输入类型(IN)、累加器类型(ACC)和输出类型(OUT)。
    //Tuple3<String, String, Long>：班级名称、学生名称、学生分数
    //Tuple2<Long, Long>：学生总分数、学生人数
    //Double：平均分数
    public static class AvgAggregate implements AggregateFunction<Tuple3<String, String, Long>, Tuple2<Long, Long>, Double> {

        /**
         * sum count
         *
         * @return
         */
        @Override
        public Tuple2<Long, Long> createAccumulator() {
            return new Tuple2<>(0L, 0L);
        }

        /**
         * 将一个输入元素添加到一个累加器中
         *
         * @param value input type
         * @param acc   acc type
         * @return
         */
        @Override
        public Tuple2<Long, Long> add(Tuple3<String, String, Long> value, Tuple2<Long, Long> acc) {
            return new Tuple2<>(acc.f0 + value.f2, acc.f1 + 1L);
        }

        /**
         * 从累加器中提取数据
         *
         * @param accumulator
         * @return
         */
        @Override
        public Double getResult(Tuple2<Long, Long> accumulator) {
            return (double) accumulator.f0 / accumulator.f1;
        }

        /**
         * 累加器合并
         *
         * @param acc1
         * @param acc2
         * @return
         */
        @Override
        public Tuple2<Long, Long> merge(Tuple2<Long, Long> acc1, Tuple2<Long, Long> acc2) {
            return Tuple2.of(acc1.f0 + acc2.f0, acc1.f1 + acc2.f1);
        }
    }


}
