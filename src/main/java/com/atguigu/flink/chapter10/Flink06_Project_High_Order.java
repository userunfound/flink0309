package com.atguigu.flink.chapter10;

import com.atguigu.flink.bean.OrderEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternFlatSelectFunction;
import org.apache.flink.cep.PatternFlatTimeoutFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.nfa.aftermatch.AfterMatchSkipStrategy;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.hadoop.yarn.webapp.hamlet2.Hamlet;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @Author: dsy
 * @Date: 2022/8/18 17:46
 * @Desciption:
 */


public class Flink06_Project_High_Order {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        KeyedStream<OrderEvent, Long> stream = env
                .readTextFile("input/OrderLog.csv")
                .map(line -> {
                    String[] data = line.split(",");
                    return new OrderEvent(
                            Long.valueOf(data[0]),
                            data[1],
                            data[2],
                            Long.parseLong(data[3]) * 1000
                    );
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<OrderEvent>forBoundedOutOfOrderness(Duration.ofSeconds(3))
                                .withTimestampAssigner((log, ts) -> log.getEventTime())
                )
                .keyBy(OrderEvent::getOrderId);

        Pattern<OrderEvent, OrderEvent> pattern = Pattern
                .<OrderEvent>begin("create", AfterMatchSkipStrategy.skipPastLastEvent())
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "create".equals(value.getEventType());
                    }
                }).optional()
                .next("pay")
                .where(new SimpleCondition<OrderEvent>() {
                    @Override
                    public boolean filter(OrderEvent value) throws Exception {
                        return "pay".equals(value.getEventType());
                    }
                })
                .within(Time.minutes(30));

        PatternStream<OrderEvent> ps = CEP.pattern(stream, pattern);

        SingleOutputStreamOperator<OrderEvent> normal = ps.flatSelect(
                new OutputTag<OrderEvent>("timeout") {
                },
                (PatternFlatTimeoutFunction<OrderEvent, OrderEvent>) (pattern1, timeoutTimestamp, out) -> {
                    OrderEvent e = pattern1.get("create").get(0);
                    out.collect(e);
                },
                (PatternFlatSelectFunction<OrderEvent, OrderEvent>) (pattern12, out) -> {
                    if (!pattern12.containsKey("create")) {
                        out.collect(pattern12.get("pay").get(0));
                    }
                }
        );

        normal.getSideOutput(new OutputTag<OrderEvent>("timeout") {}).union(normal)
                .keyBy(OrderEvent::getOrderId)
                .process(new KeyedProcessFunction<Long, OrderEvent, String>() {

                    private ValueState<OrderEvent> createState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        createState = getRuntimeContext().getState(new ValueStateDescriptor<OrderEvent>("createState", OrderEvent.class));
                    }

                    @Override
                    public void processElement(OrderEvent value,
                                               Context ctx,
                                               Collector<String> out) throws Exception {
                        if ("create".equals(value.getEventType())) {
                            createState.update(value);
                        }else{
                            if (createState.value() != null) {
                                out.collect(value.getOrderId() + "只有pay没有create");
                            }else{
                                out.collect(value.getOrderId() + "被超时支付");
                            }
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
