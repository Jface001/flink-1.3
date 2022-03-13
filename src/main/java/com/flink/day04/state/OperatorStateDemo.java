package com.flink.day04.state;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @author jface
 * @create 2022/3/13 11:43
 * @desc 需求：自定义数据源, 实现不断产生数据,并且可以进行checkpoint,保存数据的偏移量到ListState中
 * 对累加值进行状态保存, 并手动抛出异常查看
 */
public class OperatorStateDemo {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(4000);

        env.setParallelism(1);

        //2. source
        DataStreamSource<Integer> lines = env.addSource(new MySourceWithSate());

        //3. transform


        //4. sink
        lines.print();


        //5. execute
        env.execute();
    }


    public static class MySourceWithSate extends RichSourceFunction<Integer> implements CheckpointedFunction {
        // state object
        private ListState<Integer> listState = null;
        // flag
        private boolean flag = true;
        // current counter
        private Integer currentCounter = 0;

        /**
         * 将state中的数据持久化保存到文件中（每5s进行一次快照操作，将state的数据快照保存）
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState");
            // clear state
            this.listState.clear();
            // add state
            this.listState.add(this.currentCounter);

        }

        /**
         * 初始化操作，初始化state对象以及需要从持久化文件中获取已经计算过的历史数据
         *
         * @param context
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            System.out.println("initializeState");
            // get state
            OperatorStateStore stateStore = context.getOperatorStateStore();
            listState = stateStore.getListState(
                    new ListStateDescriptor<Integer>("operator-state", TypeInformation.of(new TypeHint<Integer>() {
                    }))
            );
            // get history state
            for (Integer counter : this.listState.get()) {
                this.currentCounter = counter;
            }

            // clear state
            listState.clear();
        }

        /**
         * 生成数据核心方法
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {

            while (flag) {
                currentCounter++;

                ctx.collect(currentCounter);

                TimeUnit.SECONDS.sleep(1L);

                if (this.currentCounter == 10) {
                    System.out.println("抛出异常" + (1 / 0));
                }
            }
        }

        /**
         * 取消方法
         */
        @Override
        public void cancel() {
            flag = false;
        }

    }

}

