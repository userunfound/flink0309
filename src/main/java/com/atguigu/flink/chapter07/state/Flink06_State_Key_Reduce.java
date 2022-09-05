package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: dsy
 * @Date: 2022/8/11 21:35
 * @Desciption:
 */


public class Flink06_State_Key_Reduce {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .socketTextStream("hadoop162", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ReducingState<WaterSensor> vcSumState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        vcSumState = getRuntimeContext().getReducingState(new ReducingStateDescriptor<WaterSensor>(
                                "vcSumState",
                                new ReduceFunction<WaterSensor>() {
                                    @Override
                                    public WaterSensor reduce(WaterSensor value1,
                                                              WaterSensor value2) throws Exception {
                                        value1.setVc(value1.getVc() + value2.getVc());
                                        return value1;
                                    }

                                },
                                WaterSensor.class
                        ));
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        vcSumState.add(value);

                        out.collect(vcSumState.get().toString());

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
