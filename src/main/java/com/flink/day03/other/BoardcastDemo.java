package com.flink.day03.other;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @author jface
 * @create 2022/3/8 22:04
 * @desc Boardcast
 */
public class BoardcastDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataSource<Tuple2<Integer, String>> studentInfoDataSet = env.fromElements(
                Tuple2.of(1, "王大锤"),
                Tuple2.of(2, "潇潇"),
                Tuple2.of(3, "甜甜")
        );
        // 准备分数信息数据集
        DataSource<Tuple3<Integer, String, Integer>> scoreInfoDataSet = env.fromElements(
                Tuple3.of(1, "数据结构", 99),
                Tuple3.of(2, "英语", 100),
                Tuple3.of(3, "C++", 96),
                Tuple3.of(5, "Java", 97),
                Tuple3.of(3, "Scala", 100)
        );

        //3. transform
        //广播变量的使用分为两步
        //1. 设置它, 需要在执行具体算子的后面链式调用withBroadcastSet方法
        //2. 得到它, 在算子内部getRuntimeContext().getBroadcastVariable(广播变量名)来获取

        MapOperator<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>> result = scoreInfoDataSet.map(new RichMapFunction<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>>() {
            // result map
            Map<Integer, String> map = new HashMap<Integer, String>();


            // ONLY RUN ONCE
            @Override
            public void open(Configuration parameters) throws Exception {
                List<Tuple2<Integer, String>> broadcastVariable = getRuntimeContext().getBroadcastVariable("student");
                for (Tuple2<Integer, String> stu : broadcastVariable) {
                    this.map.put(stu.f0, stu.f1);
                }
            }

            @Override
            public Tuple3<String, String, Integer> map(Tuple3<Integer, String, Integer> value) throws Exception {
                int stuId = value.f0;
                String stuName = this.map.getOrDefault(stuId, "未知");
                return Tuple3.of(stuName, value.f1, value.f2);
            }
        }).withBroadcastSet(studentInfoDataSet, "student");

        //4. sink
        result.print();

        //5. execute
    }
}
