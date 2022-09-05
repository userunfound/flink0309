package com.atguigu.flink.chapert02;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.*;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;


/**
 * @desciption:
 * @author:
 * @date:
 */


public class Flink01_BatchWordcount {
    public static void main(String[] args) throws Exception {


        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> dataSource = env.readTextFile("input/words.txt");

        FlatMapOperator<String, String> wordFMO = dataSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(word);
                }
            }
        });

        MapOperator<String, Tuple2<String, Long>> wordTuple = wordFMO.map(new MapFunction<String, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(String word) throws Exception {

                return Tuple2.of(word, 1l);
            }
        });

        UnsortedGrouping<Tuple2<String, Long>> wordUG = wordTuple.groupBy(0);

        AggregateOperator<Tuple2<String, Long>> wordAgg = wordUG.sum(1);

        wordAgg.print();
    }

}

