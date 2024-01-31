package com.echo.word_count;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class DataStreamUnboundedWordCount {
    //读取无界流
    public static void main(String[] args) throws Exception {
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
}
