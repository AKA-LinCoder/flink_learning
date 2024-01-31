//package com.echo;
//
//import com.ververica.cdc.connectors.mysql.source.MySqlSource;
//import com.ververica.cdc.connectors.mysql.table.StartupOptions;
//import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
//import org.apache.flink.api.common.eventtime.WatermarkStrategy;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.Properties;
//
//public class MysqlCDC {
//    public static void main(String[] args) throws Exception {
//        Properties debeziumProperties = new Properties();
//        debeziumProperties.put("decimal.handling.mode", "String");
////        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
////                .hostname("localhost")
////                .port(3306)
////                .scanNewlyAddedTableEnabled(true) // 开启支持新增表
////                .databaseList("user") // set captured database
////                .tableList("user.user_1,user.user_2,user.user_3") // set captured table
////                .username("test_cdc")
////                .password("tsl")
////                .debeziumProperties(debeziumProperties)
////                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
////                .build();
//        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
//                .hostname("192.168.1.133")
//                .port(3306)
//                .username("root")
//                .password("Estim@b509")
//                .databaseList("cdc_test")
//                .tableList("cdc_test.user_info")
//                .deserializer(new JsonDebeziumDeserializationSchema())
//                .startupOptions(StartupOptions.initial())
//                .build();
//
//        Configuration configuration = new Configuration();
//        // 生产环境夏下，改成参数传进来
//        configuration.setString("execution.savepoint.path","file:///tmp/flink-ck/1980d53f557a886f885172bcdf4be8e8/chk-21");
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(configuration);
//
//        // enable checkpoint
//        env.enableCheckpointing(3000);
//        // 设置本地
//        env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-ck");
//        env
//                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
//                // set 4 parallel source tasks
//                .setParallelism(4)
//                .print("==>").setParallelism(1); // use parallelism 1 for sink to keep message ordering
//
//        env.execute("Print MySQL Snapshot + Binlog");
//    }
//}