package com.echo;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Main {
    public static void main(String[] args) throws Exception {
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
}