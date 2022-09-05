package com.atguigu.flink.chapter07.watermark;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.List;

/**
 * @Author: dsy
 * @Date: 2022/8/11 10:50
 * @Desciption:
 */


public class Flink05_SideOut_1 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.getConfig().setAutoWatermarkInterval(2000);

        SingleOutputStreamOperator<String> main = env
                .socketTextStream("hadoop162", 8888)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(
                            data[0],
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[2])
                    );
                })
                .process(new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        if ("sensor_1".equals(value.getId())) {
                            out.collect(value.toString());
                        }else if("sensor_2".equals(value.getId())){
                            ctx.output(new OutputTag<String>("s2"){},value.toString() + "aaa");
                        }else{
                            ctx.output(new OutputTag<String>("other"){}, value.toString());
                        }
                    }
                });
        main.print("main");
        DataStream<String> s2 = main.getSideOutput(new OutputTag<String>("s2") {});
        s2.print();
        main.getSideOutput(new OutputTag<String>("other"){}).print("other");


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }



    }
}
