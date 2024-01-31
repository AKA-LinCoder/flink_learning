package com.echo.cdc;//package com.echo;


import com.ibm.icu.impl.Row;
//import com.ververica.cdc.connectors.mysql.MySqlSource;
//import com.ververica.cdc.connectors.mysql.source;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import org.apache.flink.configuration.Configuration;

import java.util.Properties;


public class DataStreamCDC {
    public static void main(String[] args) throws Exception {
        flinkDataStreamCDC();
    }

    public static void flinkDataStreamCDC() throws Exception {
        Configuration configuration = new Configuration();
        configuration.setInteger(RestOptions.PORT,9091);//指定 Flink Web UI 端口为9091
        //1 获取flink执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);
        //数据源的并行度
        environment.setParallelism(1);
        //1.1开启CK
        //checkPoint:检查点：防止数据丢失，确保在发生故障时可以从上一个检查点的状态继续处理数据，而不是从头开始
        //每 5s 做一次 checkpoint
        environment.enableCheckpointing(5000);
        //CheckpointingMode.EXACTLY_ONCE 表示将检查点模式设置为 "仅一次精确"，即确保每个检查点都能够精确地记录应用程序状态的一致性
        //AT_LEAST_ONCE 该模式允许在进行检查点时可能会发生重复记录，但在失败时可以保证至少一次处理
        environment.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 同一时间只能允许有一个 checkpoint
        environment.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 设置重启策略
        environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(
                30, // 重试次数
                Time.seconds(10) // 重试间隔
        ));
        //使用 Debezium 连接器时设置一些配置属性
        //指定了在处理 DECIMAL 类型数据时采用的处理模式
        //DECIMAL 类型通常用于存储精确的十进制数，而在流处理中，对于这种类型的处理方式可能涉及到一些精度和性能的权衡
        Properties debeziumProperties = new Properties();
        debeziumProperties.put("decimal.handling.mode", "String");
        //2 通过flink CDC构建SourceFunction
        MySqlSource sqlSource = MySqlSource.<String>builder()
                .hostname("192.168.1.133")
                .port(3306)
                .username("root")
                .password("Estim@b509")
                .databaseList("cdc_test")
                .tableList("cdc_test.user_info")
                .debeziumProperties(debeziumProperties)
                .scanNewlyAddedTableEnabled(true) // 开启支持新增表
                .deserializer(new CustomerDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        //1 第一个参数 ：源
        //2 第二个参数：时间标志位，设置水印（Watermark）策略的地方。在流处理中，水印用于处理事件时间（event time）和实现基于时间的操作
        //2.1
        //紧跟最大事件时间的 watermark 生成策略（完全不容忍乱序）
        //WatermarkStrategy.forMonotonousTimestamps();
        //允许乱序的 watermark 生成策略（最大事件时间-容错时间）
        //WatermarkStrategy.forBoundedOutOfOrderness(Duration.ofSeconds(10)); // 根据实际数据的最大乱序情况来设置
        //自定义 watermark 生成策略
        //WatermarkStrategy.forGenerator(new WatermarkGenerator(){ ... } );
        //WatermarkStrategy.noWatermarks()
        //3 给数据流起的一个名称，用于标识这个数据流
        DataStreamSource streamSource = environment.fromSource(sqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        //3 数据打印
        //.print("==>"): 这行代码将数据源的输出进行打印，并且设置了打印的前缀为 "==>"。print 是一个 Flink 的算子，用于将数据打印到标准输出或其他指定的输出目标
        //.setParallelism(1): 这行代码设置了 print 算子的并行度为 1，即使用一个并行任务进行打印。这样可以确保打印的顺序和原始数据源的顺序一致
        streamSource.print("==>").setParallelism(1);
        //4 启动任务
        environment.execute("FlinkCDC");
    }
    public static void flinkSqlCDC() throws Exception{
        /// flinkSql一次只能读一张表
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(environment);
        tableEnvironment.executeSql("CREATE TABLE user_info("+
                    "id STRING primary key,"+
                    "name STRING,"+") WITH ("+
                    " 'connector' = 'mysql-cdc',"+
                    " 'hostname' = '192.168.1.133', "+
                " 'scan.startup.mode' = 'latest-offset',"+
                " 'port' = '3306',"+
                " 'username' = 'root',"+
                " 'password' = 'Estim@b509',"+
                " 'database-name' = 'cdc_test',"+
                " 'table-name' = 'user_info',"
                +")");
        Table table = tableEnvironment.sqlQuery("select * from user_info");
        DataStream<Tuple2<Boolean, Row>> retractStream = tableEnvironment.toRetractStream(table, Row.class);
        retractStream.print();

        environment.execute("flinkSql");
    }

}