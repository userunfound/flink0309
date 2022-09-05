package com.atguigu.flink.chapert05.sink;

import com.alibaba.fastjson.JSON;
import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.elasticsearch.common.xcontent.XContentType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: dsy
 * @Date: 2022/8/10 19:58
 * @Desciption:
 */


public class Flink_03_Es {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);


        ArrayList<WaterSensor> waterSensors = new ArrayList<>();
        waterSensors.add(new WaterSensor("sensor_1", 1607527992000L, 20));
        waterSensors.add(new WaterSensor("sensor_1", 1607527994000L, 50));
        waterSensors.add(new WaterSensor("sensor_1", 1607527996000L, 50));
        waterSensors.add(new WaterSensor("sensor_2", 1607527993000L, 10));
        waterSensors.add(new WaterSensor("sensor_2", 1607527995000L, 30));


        DataStreamSource<WaterSensor> stream = env.fromCollection(waterSensors);

        SingleOutputStreamOperator<WaterSensor> result = stream
                .keyBy(WaterSensor::getId)
                .sum("vc");

        List<HttpHost> hosts = Arrays.asList(
                new HttpHost("hadoop162", 9200),
                new HttpHost("hadoop163", 9200),
                new HttpHost("hadoop164", 9200)
        );

        ElasticsearchSink.Builder<WaterSensor> builder = new ElasticsearchSink.Builder<WaterSensor>(
                hosts,
                new ElasticsearchSinkFunction<WaterSensor>() {
                    @Override
                    public void process(WaterSensor element,
                                        RuntimeContext ctx,
                                        RequestIndexer indexer) {
                        String msg = JSON.toJSONString(element);

                        IndexRequest ir = Requests
                                .indexRequest("sensor")
                                .type("_doc")
                                .id(element.getId())
                                .source(msg, XContentType.JSON);

                        indexer.add(ir);
                    }
                }
        );

        result.addSink(builder.build());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
