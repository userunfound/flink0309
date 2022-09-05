package com.atguigu.flink.chapter08;

import com.atguigu.flink.bean.HotItem;
import com.atguigu.flink.bean.UserBehavior;
import com.atguigu.flink.util.AtguiguUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.List;

/**
 * @Author: dsy
 * @Date: 2022/8/8 20:45
 * @Desciption:
 */


public class Flink_03_Project_High_TopN {
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
                .keyBy(UserBehavior::getItemId)
                .window(SlidingEventTimeWindows.of(Time.hours(2),Time.hours(1)))
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
                        new ProcessWindowFunction<Long, HotItem, Long, TimeWindow>() {

                            @Override
                            public void process(Long itemId,
                                                Context ctx,
                                                Iterable<Long> elements,
                                                Collector<HotItem> out) throws Exception {
                                HotItem hotItem = new HotItem(itemId, ctx.window().getEnd(), elements.iterator().next());
                                out.collect(hotItem);
                            }
                        }
                )
                .keyBy(HotItem::getWEnd)
                .process(new KeyedProcessFunction<Long, HotItem, String>() {

                    private ListState<HotItem> hotItemState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hotItemState = getRuntimeContext().getListState(new ListStateDescriptor<HotItem>("hotItemState", HotItem.class));
                    }

                    @Override
                    public void processElement(HotItem value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        if (!hotItemState.get().iterator().hasNext()) {
                            ctx.timerService().registerEventTimeTimer(value.getWEnd() + 2000);
                        }

                        hotItemState.add(value);


                    }

                    @Override
                    public void onTimer(long timestamp,
                                        OnTimerContext ctx,
                                        Collector<String> out) throws Exception {
                        List<HotItem> hotItems = AtguiguUtil.toList(hotItemState.get());
                        hotItems.sort(((o1, o2) -> o2.getCount().compareTo(o1.getCount())));

                        String msg = "\n---------------\n";
                        for (int i = 0; i < Math.min(3, hotItems.size()); i++) {
                            HotItem hotItem = hotItems.get(i);
                            msg += hotItem + "\n";
                        }
                        out.collect(msg);
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
