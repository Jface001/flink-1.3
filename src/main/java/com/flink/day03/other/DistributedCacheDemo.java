package com.flink.day03.other;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.HashMap;
import java.util.Map;

/**
 * @author jface
 * @create 2022/3/9 21:45
 * @desc Distributed Cache
 */
public class DistributedCacheDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //2. source
        DataSource<Tuple3<Integer, String, Integer>> scoreInfoDataSet = env.fromElements(
                Tuple3.of(1, "数据结构", 99),
                Tuple3.of(2, "英语", 100),
                Tuple3.of(3, "C++", 96),
                Tuple3.of(5, "Java", 97),
                Tuple3.of(3, "Scala", 100)
        );

        /*
        分布式缓存和广播变量的使用步骤基本差不多，有一点不同
        1. 设置它, 使用env.registerCachedFile来注册分布式缓存.
        2. 使用它, 在算子内部调用getRuntimeContext.getDistributedCache.getFile(File)来获取分布式缓存的文件
         */

        //3. transform
        env.registerCachedFile("hdfs://node01:8020/flink/cache/distribute_cache_student", "student");

        MapOperator<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>> result = scoreInfoDataSet.map(new RichMapFunction<Tuple3<Integer, String, Integer>, Tuple3<String, String, Integer>>() {
            final Map<Integer, String> map = new HashMap<Integer, String>();

            @Override
            public void open(Configuration parameters) throws Exception {
                super.open(parameters);

                File student = getRuntimeContext().getDistributedCache().getFile("student");
                BufferedReader bufferedReader = new BufferedReader(new FileReader(student));

                // lambda
/*                bufferedReader.lines().forEach((String line)->{
                    String[] words = line.split(" ");
                    map.put(Integer.parseInt(words[0]), words[1]);*/

                // usually
                String line = null;
                while ((line = bufferedReader.readLine()) != null) {
                    String[] words = line.split(" ");
                    map.put(Integer.parseInt(words[0]), words[1]);

                }


            }


            @Override
            public Tuple3<String, String, Integer> map(Tuple3<Integer, String, Integer> value) throws Exception {
                return Tuple3.of(map.getOrDefault(value.f0, "未知学生姓名"), value.f1, value.f2);
            }
        });

        //4. sink
        result.print();
        //5. execute
    }
}
