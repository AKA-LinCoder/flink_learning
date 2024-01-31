package com.echo.word_count;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class DataStreamBoundedWordCount {
    //基于DataStream,有界流
    public static void main(String[] args) throws Exception {
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
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = textFile.flatMap((FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
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
