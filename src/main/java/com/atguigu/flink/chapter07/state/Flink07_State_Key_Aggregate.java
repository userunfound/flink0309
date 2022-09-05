package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.chapter07.window.Flink03_Window_Process_Agg;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: dsy
 * @Date: 2022/8/11 21:51
 * @Desciption:
 */


public class Flink07_State_Key_Aggregate {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
            .socketTextStream("hadoop162", 9999) // socket只能是1
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

                private AggregatingState<WaterSensor, Double> vcAvgState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    vcAvgState = getRuntimeContext().getAggregatingState(new AggregatingStateDescriptor<WaterSensor, Avg, Double>(
                            "vcAvgState",
                            new AggregateFunction<WaterSensor, Avg, Double>() {
                                @Override
                                public Avg createAccumulator() {
                                    return new Avg();
                                }

                                @Override
                                public Avg add(WaterSensor value, Avg acc) {
                                    acc.sum += value.getVc();
                                    acc.count++;
                                    return acc;
                                }

                                @Override
                                public Double getResult(Avg acc) {
                                    return acc.avg();
                                }

                                @Override
                                public Avg merge(Avg a, Avg b) {
                                    return null;
                                }
                            },
                            Avg.class
                    ));
                }

                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    vcAvgState.add(value);

                    out.collect(ctx.getCurrentKey() + " 的平均水位:" + vcAvgState.get());

                }
            })
            .print();


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }

    public static class Avg {
        public Integer sum = 0;
        public Long count = 0L;

        public Double avg() {
            return sum * 1.0 / count;
        }
    }
}
