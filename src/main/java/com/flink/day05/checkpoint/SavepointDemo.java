package com.flink.day05.checkpoint;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * savepoint的目的是为了从上一次保存的中间结果中恢复过来
 * 举例：
 * 在生产环境中运行着一个作业，因为今晚要升级作业，因此需要将生产环境的作业停止掉，将升级后的jar进行部署和发布
 * 希望重新发布以后可以将上一个作业的运行结果恢复后继续运行
 *
 * 所以这时候可以使用savepoint进行解决这个问题问题
 *
 * 面试问题：
 * checkpoint和savepoint的区别？
 * checkpoint：周期性定期运行，生成barrier（栅栏）发送到job作业的每个算子，当算子收到barrier以后会将state的中间计算结果快照存储到分布式文件系统中
 * savepoint：将指定的checkpoint的结果恢复过来，恢复到当前的作业中，继续运行
 *
 * TODO 当作业重新递交的时候，并行度发生了改变，在flink1.10版本中，可以正常的递交作业，且能够恢复历史的累加结果
 * 但是之前版本中一旦发生并行度的变化，作业无法递交
 */
public class SavepointDemo {
    public static void main(String[] args) throws Exception {
        /**
         * 实现步骤：
         * 1）初始化flink流处理的运行环境
         * 2）开启checkpoint
         * 3）指定数据源
         * 4）对字符串进行空格拆分，然后每个单词记一次数
         * 5）对每个单词进行分组聚合操作
         * 6) 打印测试
         * 7）执行任务，递交作业
         */

        //TODO 1）初始化flink流处理的运行环境
        //配置环境
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8081);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);

        //禁用job的任务连
        //env.disableOperatorChaining();


        //TODO 2）开启checkpoint
        //周期性的生成barrier（栅栏），默认情况下checkpoint是没有开启的
        env.enableCheckpointing(5000L);
        //设置checkpoint的超时时间
        env.getCheckpointConfig().setCheckpointTimeout(6000L);
        //设置同一个时间只能有一个栅栏在运行
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置checkpoint的执行模式，最多执行一次或者至少执行一次
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        //Checkpointing最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);
        //指定checkpoint的存储位置
        if(args.length < 1) {
            env.setStateBackend(new FsStateBackend("file:///D:\\checkpoint"));
        }else{
            env.setStateBackend(new FsStateBackend(args[0]));
        }

        //TODO 取消作业的时候，上一次成功checkpoint的结果，被删除了！意味着不能将上次执行累加的结果无法恢复，因此我们希望取消作业的时候，不要删除已经checkpoit成功的历史数据
        //ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION: 当作业被取消时，保留外部的checkpoint。注意，在此情况下，您必须手动清理checkpoint状态。
        //ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION: 当作业被取消时，删除外部化的checkpoint。只有当作业失败时，检查点状态才可用。
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //TODO 3）指定数据源
        DataStreamSource<String> socketTextStream = env.socketTextStream("node01", 7777);

        //TODO 4）对字符串进行空格拆分，然后每个单词记一次数
        SingleOutputStreamOperator<Tuple2<String, Long>> wordAndOne = socketTextStream.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(Tuple2.of(word, 1L));
                }
            }
        });

        //TODO 5）对每个单词进行分组聚合操作
        SingleOutputStreamOperator<Tuple2<String, Long>> sumed = wordAndOne.keyBy(0).sum(1);

        //TODO 6) 打印测试
        sumed.print();

        //TODO 7）执行任务，递交作业
        env.execute();
    }
}