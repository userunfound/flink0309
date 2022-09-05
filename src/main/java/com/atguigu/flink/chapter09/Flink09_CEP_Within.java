package com.atguigu.flink.chapter09;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.PatternTimeoutFunction;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author: dsy
 * @Date: 2022/8/14 21:05
 * @Desciption:
 */


public class Flink09_CEP_Within {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env
                .readTextFile("input/sensor.txt")
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ws, ts) -> ws.getTs())
                );

        Pattern<WaterSensor, WaterSensor> pattern = Pattern
                .<WaterSensor>begin("s1")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_1".equals(value.getId());
                    }
                })
                .next("s2")
                .where(new SimpleCondition<WaterSensor>() {
                    @Override
                    public boolean filter(WaterSensor value) throws Exception {
                        return "sensor_2".equals(value.getId());
                    }
                })
                .within(Time.seconds(2));

        PatternStream<WaterSensor> ps = CEP.pattern(stream, pattern);

        SingleOutputStreamOperator<String> normal = ps
                .select(
                        new OutputTag<WaterSensor>("late") {
                        },
                        new PatternTimeoutFunction<WaterSensor, WaterSensor>() {
                            @Override
                            public WaterSensor timeout(
                                    Map<String, List<WaterSensor>> map,
                                    long timeoutTimestamp) throws Exception {
                                return map.get("s1").get(0);
                            }
                        },
                        new PatternSelectFunction<WaterSensor, String>() {
                            @Override
                            public String select(Map<String, List<WaterSensor>> map) throws Exception {
                                return map.toString();
                            }
                        }
                );

        normal.getSideOutput(new OutputTag<WaterSensor>("late"){}).print();
        normal.print("nomal");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
