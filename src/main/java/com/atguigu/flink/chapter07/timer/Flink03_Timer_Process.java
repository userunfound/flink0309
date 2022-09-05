package com.atguigu.flink.chapter07.timer;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: dsy
 * @Date: 2022/8/13 15:37
 * @Desciption:
 */


public class Flink03_Timer_Process {
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
                            Integer.parseInt(data[2])
                    );

                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner((ws, ts) -> ws.getTs())
                )
                .keyBy(WaterSensor::getId)
                .process(new KeyedProcessFunction<String, WaterSensor, String>() {
                    private long ts;
                    int lastVc;

                    @Override
                    public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
                        out.collect(ctx.getCurrentKey() + " 连续5s上升，发出预警...");
                        lastVc = 0;
                    }

                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> collector) throws Exception {
                        if (lastVc == 0) {
                            ts = value.getTs() + 5000;
                            System.out.println("注册定时器" + ts);
                            ctx.timerService().registerEventTimeTimer(ts);
                        }else if(value.getVc() < lastVc){
                            System.out.println("水位下降，删除定时器：" + ts);
                            ctx.timerService().deleteEventTimeTimer(ts);

                            ts = value.getTs() + 5000;
                            System.out.println("重新注册定时器：" + ts);
                            ctx.timerService().registerEventTimeTimer(ts);
                        }else{
                            System.out.println("水位上升，什么都不做");
                        }

                        lastVc = value.getVc();
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
