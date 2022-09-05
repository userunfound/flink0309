package com.atguigu.flink.chapter07.state;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: dsy
 * @Date: 2022/8/11 15:20
 * @Desciption:
 */


public class Flink_State_Operator_List {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.enableCheckpointing(2000);

        env
                .socketTextStream("hadoop162", 9999)
                .map(new MyMapFuction())
                .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static class MyMapFuction implements MapFunction<String, String>, CheckpointedFunction {

        List<String> words = new ArrayList<>();
        private ListState<String> wordsState;

        @Override
        public String map(String line) throws Exception {
            if (line.contains("x")) {
                throw new RuntimeException("手动抛出异常");
            }

            String[] data = line.split(",");
            words.addAll(Arrays.asList(data));
            return words.toString();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
//            System.out.println("MyMapFuction.snapshotState");
            wordsState.update(words);
        }

        @Override
        public void initializeState(FunctionInitializationContext ctx) throws Exception {
            System.out.println("MyMapFuction.initializeState");
            wordsState = ctx.getOperatorStateStore().getListState(new ListStateDescriptor<String>("wordsState", String.class));
            Iterable<String> it = wordsState.get();
            for (String word : it) {
                words.add(word);
            }
        }
    }
}
