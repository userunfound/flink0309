package com.atguigu.flink.chapter07.state;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: dsy
 * @Date: 2022/8/11 22:27
 * @Desciption:
 */


public class Flink09_State_Key_Map {
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

                private MapState<Integer, Object> vcMapState;

                @Override
                public void open(Configuration parameters) throws Exception {
                    vcMapState = getRuntimeContext().getMapState(new MapStateDescriptor<Integer, Object>(
                            "vcMapState",
                            TypeInformation.of(new TypeHint<Integer>() {
                            }),
                            TypeInformation.of(new TypeHint<Object>() {
                            })
                    ));
                }

                @Override
                public void processElement(WaterSensor value,
                                           Context ctx,
                                           Collector<String> out) throws Exception {
                    vcMapState.put(value.getVc(), new Object());

                    Iterable<Integer> vcs = vcMapState.keys();

                    out.collect(ctx.getCurrentKey() + " 的所有不同水位:" + AtguiguUtil.toList(vcs));

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
