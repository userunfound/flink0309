package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Author: dsy
 * @Date: 2022/8/11 16:14
 * @Desciption:
 */


public class Flink_03_State_Operator_Broadcast {
    public static void main(String[] args) {
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(2);

        env.enableCheckpointing(2000);

        DataStreamSource<String> dataStream = env.socketTextStream("hadoop162", 8888);

        DataStreamSource<String> configStream = env.socketTextStream("hadoop162", 9999);

        MapStateDescriptor<String, String> bcStateDesc = new MapStateDescriptor<>("bcState", String.class, String.class);

        BroadcastStream<String> bcStream = configStream.broadcast(bcStateDesc);

        BroadcastConnectedStream<String, String> coStream = dataStream.connect(bcStream);

        coStream
                .process(new BroadcastProcessFunction<String, String, String>() {
                    @Override
                    public void processElement(String value,
                                               ReadOnlyContext ctx,
                                               Collector<String> out) throws Exception {
                        System.out.println("Flink_03_State_Operator_Broadcast.processElement");
                        ReadOnlyBroadcastState<String, String> state = ctx.getBroadcastState(bcStateDesc);

                        String conf = state.get("aSwitch");

                        if ("1".equals(conf)) {
                            out.collect(value + "使用1号逻辑");
                        }else if("2".equals(conf)){
                            out.collect(value + "使用2号逻辑");
                        }else if("3".equals(conf)){
                            out.collect(value + "使用3号逻辑");
                        }else{
                            out.collect(value + "使用default逻辑");
                        }
                    }

                    @Override
                    public void processBroadcastElement(String value,
                                                        Context ctx,
                                                        Collector<String> out) throws Exception {
                        System.out.println("Flink_03_State_Operator_Broadcast.processBroadcastElement");
                        BroadcastState<String, String> state = ctx.getBroadcastState(bcStateDesc);
                        state.put("aSwitch", value);

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
