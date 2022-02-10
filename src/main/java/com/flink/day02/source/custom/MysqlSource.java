package com.flink.day02.source.custom;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.*;
import java.util.concurrent.TimeUnit;

/**
 * @author jface
 * @create 2022/2/10 22:12
 * @desc 实现flink与mysql的整合，读取mysql的数据并输出
 */
public class MysqlSource {
    public static void main(String[] args) throws Exception {
        //1. env
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        System.out.println("执行环境默认的并行度：" + env.getParallelism());

        //2. source
        DataStreamSource<UserInfo> streamSource = env.addSource(new MysqlSourceFunction()).setParallelism(1);
        System.out.println("自定义source的并行度：" + streamSource.getParallelism());
        //3. sink
        streamSource.print();
        //4. execute
        env.execute();

    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class UserInfo {
        private int id;
        private String username;
        private String password;
        private String name;
    }

    private static class MysqlSourceFunction extends RichParallelSourceFunction<UserInfo> {
        // 1 .定义 mysql 的连接对象
        private Connection connection = null;
        // 2. 定义 statement 对象
        private PreparedStatement statement = null;
        // 3. 定义是否停止运行的标记
        private boolean isRunning = true;

        /**
         * 实例化的时候会被执行一次，多个并行度会执行多次，因为有多个实例
         * 一般用于资源的初始化操作
         *
         * @param parameters
         * @throws Exception
         */
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            // 注册驱动
            Class.forName("com.mysql.jdbc.Driver");
            // 获取连接
            connection = DriverManager.getConnection("jdbc:mysql://localhost:3306/flink", "root", "123456");
            statement = connection.prepareStatement("select * from user_info");
        }

        /**
         * 释放资源
         *
         * @throws Exception
         */
        @Override
        public void close() throws Exception {
            super.close();
            if (statement != null && !statement.isClosed()) {
                statement.close();
            }
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        }

        /**
         * 查询数据核心方法
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<UserInfo> ctx) throws Exception {
            ResultSet resultSet = statement.executeQuery();

            while (isRunning && resultSet.next()) {
                int id = resultSet.getInt("id");
                String username = resultSet.getString("username");
                String password = resultSet.getString("password");
                String name = resultSet.getString("name");

                // 返回数据
                ctx.collect(new UserInfo(id, username, password, name));
            }
            resultSet.close();

            TimeUnit.SECONDS.sleep(1);
        }

        @Override
        public void cancel() {

        }

    }
}
