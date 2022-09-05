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


public class Flink_03_Es_2 {
    public static void main(String[] args) {
        Configuration conf = new Configuration();
        conf.setInteger("rest.port", 2000);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> result = env
                .socketTextStream("hadoop162", 9999)
                .map(line -> {
                    String[] data = line.split(",");
                    return new WaterSensor(data[0], Long.valueOf(data[1]), Integer.valueOf(data[2]));
                })
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

        builder.setBulkFlushInterval(2000);
        builder.setBulkFlushMaxSizeMb(1024);
        builder.setBulkFlushMaxActions(2);

        result.addSink(builder.build());

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
