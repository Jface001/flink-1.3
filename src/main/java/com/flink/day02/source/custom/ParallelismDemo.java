package com.flink.day02.source.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;
import java.util.UUID;


/**
 * @author jface
 * @create 2022/2/10 22:12
 * @desc
 * 多并行度的自定义数据源
 * 自定义数据源, 每1秒钟随机生成一条订单信息(订单ID、用户ID、订单金额、时间戳)
 */
public class ParallelismDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        System.out.println("执行环境默认的并行度：" + env.getParallelism());

        //2. source
        //The parallelism of non parallel operator must be 1.
        //DataStreamSource<Order> streamSource = env.addSource(new MySource()).setParallelism(2);
        DataStreamSource<Order> streamSource = env.addSource(new MySource()).setParallelism(2);
        System.out.println("自定义数据源的并行度：" + streamSource.getParallelism());

        //3. sink
        streamSource.print();
        //4. execute
        env.execute();

    }


    /**
     * 定义订单的 JavaBean 对象
     */
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class Order {
        // 订单 id
        private String id;
        // 用户 id
        private String UserId;
        // 订单金额
        private int money;
        // 时间戳
        private Long timestamp;
    }

    private static class MySource implements ParallelSourceFunction<Order> {
        // 定义是否循环生成数据的标记
        private boolean isRunning = true;

        /**
         * 核心方法，生成数据
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<Order> ctx) throws Exception {
            Random random = new Random();
            while (isRunning) {
                // 生成订单数据
                String orderId = UUID.randomUUID().toString();
                String userId = String.valueOf(random.nextInt(3));
                int money = random.nextInt(1000);
                long time = System.currentTimeMillis();

                // 返回数据
                ctx.collect(new Order(orderId, userId, money, time));
                // 社会休眠时间
                Thread.sleep(1000L);
            }


        }

        /**
         * 取消数据源
         */
        @Override
        public void cancel() {
            isRunning = false;
        }

    }
}



