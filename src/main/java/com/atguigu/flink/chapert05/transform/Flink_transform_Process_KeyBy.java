package com.atguigu.flink.chapert05.transform;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.Map;

/**
 * @desciption:
 * @author:
 * @date:
 */


public class Flink_transform_Process_KeyBy {
    public static void main(String[] args) {

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> ds = env.fromElements(
                new WaterSensor("sensor_1", 1l, 10),
                new WaterSensor("sensor_1", 1l, 20),
                new WaterSensor("sensor_1", 1l, 30),
                new WaterSensor("sensor_1", 1l, 40),
                new WaterSensor("sensor_1", 1l, 50)
                );
        ds
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {

                    Map<String, Integer> map = new HashMap<>();
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {

                        Integer sum = map.getOrDefault(ctx.getCurrentKey(), 0);
                        sum += value.getVc();
                        map.put(ctx.getCurrentKey(), sum);

                        out.collect(ctx.getCurrentKey() + "的水位和：" + sum);

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
