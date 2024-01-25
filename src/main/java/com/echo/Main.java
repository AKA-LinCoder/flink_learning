package com.echo;

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


}