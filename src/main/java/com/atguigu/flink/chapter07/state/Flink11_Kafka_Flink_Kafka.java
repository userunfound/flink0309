package com.atguigu.flink.chapter07.state;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerRecord;


import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;
import java.util.Properties;

/**
 * @Author: dsy
 * @Date: 2022/8/11 22:50
 * @Desciption:
 */


public class Flink11_Kafka_Flink_Kafka {
    public static void main(String[] args) {
        System.setProperty("HADOOP_USER_NAME", "atguigu");
        Configuration conf = new Configuration();

        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);

        env.enableCheckpointing(2000);
        env.setStateBackend(new HashMapStateBackend());

        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop162:8020/ck100");

        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(500);

        env.getCheckpointConfig().setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        Properties sourceProps = new Properties();

        sourceProps.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sourceProps.put("group.id", "Flink_Kafka_Flink_Kafka");

        sourceProps.put("isolation.level", "read_committed");

        Properties sinkProps = new Properties();

        sinkProps.put("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        sinkProps.put("transaction.timeout.ms",15 * 60 * 1000);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream = env
                .addSource(
                        new FlinkKafkaConsumer<String>("s1", new SimpleStringSchema(), sourceProps)
                                .setStartFromLatest()
                ).flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {

                    @Override
                    public void flatMap(String value, Collector<Tuple2<String, Long>> out) throws Exception {
                        for (String word : value.split(" ")) {
                            out.collect(Tuple2.of(word, 1L));
                        }
                    }
                })
                .keyBy(t -> t.f0)
                .sum(1);
        stream
            .addSink(new FlinkKafkaProducer<Tuple2<String, Long>>(
                    "default",
                    new KafkaSerializationSchema<Tuple2<String, Long>>() {
                        @Override
                        public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Long> element,
                                                                        @Nullable Long timestamp) {
                            return new ProducerRecord<>("s2", (element.f0 + "_" + element.f1).getBytes(StandardCharsets.UTF_8));
                        }
                    },
                    sinkProps,
                    FlinkKafkaProducer.Semantic.EXACTLY_ONCE
            ));

        stream.addSink(new SinkFunction<Tuple2<String, Long>>() {
            @Override
            public void invoke(Tuple2<String, Long> value,
                               Context ctx) throws Exception {
                if (value.f0.contains("x")) {
                    throw new RuntimeException("手动抛出异常");
                }
            }
        });


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
