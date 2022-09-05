package com.atguigu.flink.chapert05.transform;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * @desciption:
 * @author:
 * @date:
 */


public class Flink_transform_Connect {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port",2000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> s1 = env.fromElements(10, 11, 9, 20, 12);
        DataStreamSource<String> s2 = env.fromElements("a", "b", "c");

        ConnectedStreams<Integer, String> s12 = s1.connect(s2);

        s12
                .map(new CoMapFunction<Integer, String, String>() {
                    @Override
                    public String map1(Integer value) throws Exception {
                        return value + "<";
                    }

                    @Override
                    public String map2(String value) throws Exception {
                        return value + ">";
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
