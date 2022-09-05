package com.atguigu.flink.chapert05.sink;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * @Author: dsy
 * @Date: 2022/8/11 10:27
 * @Desciption:
 */


public class Flink05_Sink_Jdbc {
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

        result.addSink(JdbcSink.sink(
                "replace into sensor(id, ts, vc) values(?,?,?)",
                new JdbcStatementBuilder<WaterSensor>() {
                    @Override
                    public void accept(PreparedStatement ps,
                                       WaterSensor waterSensor) throws SQLException {

                        ps.setString(1,waterSensor.getId());
                        ps.setLong(2,waterSensor.getTs());
                        ps.setInt(3,waterSensor.getVc());
                    }

                },
                new JdbcExecutionOptions.Builder()
                        .withBatchSize(1024)
                        .withBatchIntervalMs(2000)
                        .withMaxRetries(3)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                .withDriverName("com.mysql.jdbc.Driver")
                .withUrl("jdbc:mysql://hadoop162:3306/test?useSSL=false")
                .withUsername("root")
                .withPassword("aaaaaa")
                .build()
        ));


        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }


    }
}
