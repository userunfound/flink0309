package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.UserBehavior;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import scala.collection.immutable.$colon$colon;

import java.time.Duration;

/**
 * @Author: dsy
 * @Date: 2022/8/8 20:45
 * @Desciption:
 */


public class Flink_01_Project_High_Pv {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env
                .readTextFile("input/UserBehavior.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new UserBehavior(
                            Long.valueOf(data[0]),
                            Long.valueOf(data[1]),
                            Integer.valueOf(data[1]),
                            data[3],
                            Long.parseLong(data[4]) * 1000

                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.
                                <UserBehavior>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((ub, ts) -> ub.getTimestamp())

                )
                .filter(ub -> "pv".equals(ub.getBehavior()))
                .windowAll(SlidingEventTimeWindows.of(Time.hours(2),Time.hours(1)))
                .aggregate(
                        new AggregateFunction<UserBehavior, Long, Long>() {
                            @Override
                            public Long createAccumulator() {
                                return 0L;
                            }

                            @Override
                            public Long add(UserBehavior value, Long accumulator) {
                                return accumulator + 1;
                            }

                            @Override
                            public Long getResult(Long accumulator) {
                                return accumulator;
                            }

                            @Override
                            public Long merge(Long a, Long b) {
                                return null;
                            }
                        },
                        new ProcessAllWindowFunction<Long, String, TimeWindow>() {
                            @Override
                            public void process(Context ctx,
                                                Iterable<Long> elements,
                                                Collector<String> out) throws Exception {
                                Long result = elements.iterator().next();

                                String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                                String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());

                                out.collect(stt + " " + edt + " " + result);

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
}
