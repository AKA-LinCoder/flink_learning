package com.echo;//package com.echo;

import com.ververica.cdc.connectors.mysql.MySqlSource;
import com.ververica.cdc.debezium.DebeziumSourceFunction;
import com.ververica.cdc.debezium.StringDebeziumDeserializationSchema;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
        wordCountBatchByDataSetApi();
        wordCountStream();
        wordCountStreamUnbounded();
    }

    //基于DataSet API实现wordCount ,过时不推荐
    public static  void wordCountBatchByDataSetApi()throws Exception{
        //1 创建执行环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        //2 读取数据
        DataSource<String> textFile = env.readTextFile("input/word.txt");
        //3 按行切分，转换
        FlatMapOperator<String, Tuple2<String, Integer>> wordAndOne = textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> collector) throws Exception {
                //3.1 按照空格切分
                String[] words = value.split(" ");
                //3.2 转换成元组
                for (String word : words) {
                    Tuple2<String, Integer> wordTuple2 = Tuple2.of(word, 1);
                    //3.3使用collector向下游发送数据
                    collector.collect(wordTuple2);
                }
            }
        });
        //4 按照word分组
        UnsortedGrouping<Tuple2<String, Integer>> wordAndOneGroupBy = wordAndOne.groupBy(0);
        //5 各分组内聚合
        AggregateOperator<Tuple2<String, Integer>> sum = wordAndOneGroupBy.sum(1); //1是位置，表示第二个元素
        //6 输出
        sum.print();
    }

    //基于DataStream,有界流
    public static void wordCountStream() throws Exception {
        //1 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //2 读取数据
        DataStreamSource<String> textFile = env.readTextFile("input/word.txt");
        //3 处理数据
//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = textFile.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public void flatMap(String s, Collector<Tuple2<String, Integer>> collector) throws Exception {
//                String[] strings = s.split(" ");
//                for (String word : strings) {
//                    Tuple2<String, Integer> stringIntegerTuple2 = Tuple2.of(word, 1);
//                    collector.collect(stringIntegerTuple2);
//                }
//            }
//        });

        ///使用Lambda 表达式，必须手动指定输出类型Types.TUPLE(Types.STRING, Types.INT)
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = textFile.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) ( s,collector) -> {
            String[] strings = s.split(" ");
            for (String word : strings) {
                Tuple2<String, Integer> stringIntegerTuple2 = Tuple2.of(word, 1);
                collector.collect(stringIntegerTuple2);
            }
        }, Types.TUPLE(Types.STRING,Types.INT));
        // 分组
        KeyedStream<Tuple2<String, Integer>, Object> keyBy = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, Object>() {
            @Override
            public Object getKey(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                return stringIntegerTuple2.f0;
            }
        });
        // 聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = keyBy.sum(1);
        //4 输出数据
        sum.print();
        //5 执行类似 sparkStreaming最后ssc.start（）
        env.execute();


        /// 3> 表示并行度，线程数
        //3> (hello,1)
        //2> (java,1)
        //7> (flink,1)
        //5> (world,1)
        //3> (hello,2)
        //3> (hello,3)

    }


    //读取无界流
    public static  void wordCountStreamUnbounded() throws Exception{
        //创建执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //socket读取数据
        DataStreamSource<String> socketTextStream = env.socketTextStream("hadoop102", 7777);
        //处理数据
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = socketTextStream.flatMap((String value, Collector<Tuple2<String, Integer>> collector) -> {
            String[] words = value.split(" ");
            for (String word : words) {
                collector.collect(Tuple2.of(word, 1));
            }
        }).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy((Tuple2<String, Integer> value) -> value.f0).sum(1);
        //输出
        sum.print();
        //执行
        env.execute();
        // 事件驱动：有数据来才有下一步
    }


    public static void flinkCDC() throws Exception {
        //1 获取flink执行环境
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();
        environment.setParallelism(1);
        //2 通过flink CDC构建SourceFunction
        DebeziumSourceFunction<String> sourceFunction = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .username("root")
                .password("000000")
                .databaseList("cdc_test")
                .tableList("cdc_test.table_a")
                .deserializer(new StringDebeziumDeserializationSchema())
                .build();
        DataStreamSource<String> dataStreamSource = environment.addSource(sourceFunction);
        //3 数据打印
        dataStreamSource.print();

        //4 启动任务
        environment.execute("FlinkCDC");


    }


}