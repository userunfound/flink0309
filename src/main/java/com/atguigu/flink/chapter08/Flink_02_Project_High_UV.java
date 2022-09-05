package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.UserBehavior;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Author: dsy
 * @Date: 2022/8/8 20:45
 * @Desciption:
 */


public class Flink_02_Project_High_UV {
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
                .process(new ProcessAllWindowFunction<UserBehavior, String, TimeWindow>() {

                    private MapState<Long, Object> userIdState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        userIdState = getRuntimeContext().getMapState(
                                new MapStateDescriptor<Long, Object>("userIdState", Long.class, Object.class)
                        );
                    }

                    @Override
                    public void process(Context ctx,
                                        Iterable<UserBehavior> elements,
                                        Collector<String> out) throws Exception {

                        for (UserBehavior element : elements) {
                            userIdState.put(element.getUserId(), new Object());
                        }

                        String stt = AtguiguUtil.toDateTime(ctx.window().getStart());
                        String edt = AtguiguUtil.toDateTime(ctx.window().getEnd());

                        out.collect(stt + " " + edt + " " + AtguiguUtil.toList(userIdState.keys()).size());

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
