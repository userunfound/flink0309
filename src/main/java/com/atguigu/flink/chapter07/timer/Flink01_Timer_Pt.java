package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: dsy
 * @Date: 2022/8/13 11:29
 * @Desciption:
 */


public class Flink01_Timer_Pt {
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

                    private long ts;

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 的水位超过10，发出预警...");
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        if (value.getVc() > 10) {
                            ts = System.currentTimeMillis() + 5000;
                            System.out.println("注册定时器：" + ts);
                            ctx.timerService().registerProcessingTimeTimer(ts);
                        }else if(value.getVc() < 10){
                            System.out.println("取消定时器：" + ts);
                            ctx.timerService().deleteProcessingTimeTimer(ts);
                        }

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
