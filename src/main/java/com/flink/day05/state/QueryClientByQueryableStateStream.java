package com.flink.day05.state;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.queryablestate.client.QueryableStateClient;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;

public class QueryClientByQueryableStateStream {
    public static void main(String[] args) throws Exception {

        if (args.length == 0) {
            throw new IllegalArgumentException(
                    "Missing Flink Job ID ...\n"
                            + "Usage: ./QueryClient <jobID> [jobManagerHost] [jobManagerPort]");
        }

        final JobID jobId = JobID.fromHexString(args[0]);
        final String jobManagerHost = args.length > 1 ? args[1] : "localhost";
        final int jobManagerPort = args.length > 1 ? Integer.parseInt(args[1]) : 9069;

        QueryableStateClient client = new QueryableStateClient(jobManagerHost, jobManagerPort);

    /*
      状态描述符
      event_five_seconds_max_pv 状态名称，Server端此名称是通过UUID生成的，这里随意即可
      TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {})) 状态中值的类型，应和Server端相同
     */
        ValueStateDescriptor<Tuple2<String, Long>> stateDescriptor =
                new ValueStateDescriptor<>(
                        "lastFiveSecondsMaxPV",
                        TypeInformation.of(new TypeHint<Tuple2<String, Long>>() {
                        }));
        final String key = "spark";

    /*
     轮询执行查询
     event_five_seconds_max_pv queryableStateName，应和Server端的queryableStateName相同
    */
        while (true) {
            CompletableFuture<ValueState<Tuple2<String, Long>>> completableFuture =
                    client.getKvState(
                            jobId,
                            "lastFiveSecondsMaxPV",
                            key,
                            BasicTypeInfo.STRING_TYPE_INFO,
                            stateDescriptor);

            completableFuture.thenAccept(response -> {
                try {
                    Tuple2<String, Long> value = response.value();
                    System.out.println(value);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });

            Thread.sleep(1000);
        }
    }
}
