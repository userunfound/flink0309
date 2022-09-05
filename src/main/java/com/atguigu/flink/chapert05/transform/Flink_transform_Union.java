package com.atguigu.flink.chapert05.transform;

import org.apache.commons.math3.fitting.leastsquares.EvaluationRmsChecker;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @desciption:
 * @author:
 * @date:
 */


public class Flink_transform_Union {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> s1 = env.fromElements(10, 11, 9, 20, 12);
        DataStreamSource<Integer> s2 = env.fromElements(110, 111, 19, 120, 12);
        DataStreamSource<Integer> s3 = env.fromElements(1110, 1111, 191, 1120, 112);

        DataStream<Integer> s123 = s1.union(s2, s3);

        s123
                .map(new MapFunction<Integer, String>() {
                    @Override
                    public String map(Integer value) throws Exception {

                        return value + "<>";
                    }
                })
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
