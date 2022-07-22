package com.flink.day05.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.concurrent.CompletableFuture;

/**
 * 客户端程序查询flink计算的状态结果
 */
public class QueryStateClientDemo {
    public static void main(String[] args) throws UnknownHostException, InterruptedException {
        //创建flinkstate的代理客户端对象（指定代理的地址和端口号）
        QueryableStateClient client = new QueryableStateClient("localhost", 9069);

        //初始化状态数据对象
        ValueStateDescriptor valueStateDescriptor = new ValueStateDescriptor(
                "lastMaxPV",
                Integer.class
        );

        CompletableFuture<ValueState<Integer>> resultFuture = client.getKvState(
                JobID.fromHexString("57616eef58ddde93ead8239dda747e3d"), //jobid
                "lastMaxPV",//可查询的state的名称
                "spark", //查询key，spark累加值
                BasicTypeInfo.STRING_TYPE_INFO,
                valueStateDescriptor
        );

        resultFuture.thenAccept(response -> {
            try {
                Integer value = response.value();
                System.out.println(value);
            } catch (IOException e) {
                e.printStackTrace();
            }
        });
        Thread.sleep(5000);
    }
}
