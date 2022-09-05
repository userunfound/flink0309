package com.atguigu.flink.chapter07.window;

import com.atguigu.flink.bean.WaterSensor;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.graph.AbstractValueGraph;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @author:dsy
 * @date:2022/8/4 23:03
 * @desciption:
 */


public class Flink03_Window_Process_Agg {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
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
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .aggregate(
                        new AggregateFunction<WaterSensor, Avg, Double>() {
                            @Override
                            public Avg createAccumulator() {
                                System.out.println("Flink03_Window_Process_Agg.createAccumulator");
                                return new Avg();
                            }

                            @Override
                            public Avg add(WaterSensor value, Avg acc) {
                                System.out.println("Flink03_Window_Process_Agg.add");
                                acc.sum += value.getVc();
                                acc.count++;
                                return acc;

                            }

                            @Override
                            public Double getResult(Avg acc) {
                                System.out.println("Flink03_Window_Process_Agg.getResult");
                                return acc.sum * 1.0 / acc.count;
                            }

                            @Override
                            public Avg merge(Avg a, Avg b) {
                                System.out.println("Flink03_Window_Process_Agg.merge");
                                return null;
                            }
                        },
                        new ProcessWindowFunction<Double, String, String, TimeWindow>() {
                            @Override
                            public void process(String key,
                                                Context ctx,
                                                Iterable<Double> elements,
                                                Collector<String> out) throws Exception {
                                System.out.println("Flink03_Window_Process_Agg.process");
                                Double result = elements.iterator().next();

                                String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                                String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());

                                out.collect(key + " " + stt + " " + edt +  " " + result);

                            }
                        }
                )
                .print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class Avg {
        public  Integer sum = 0;
        public  Long count = 0L;
    }
}
