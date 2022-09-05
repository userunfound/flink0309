package com.atguigu.flink.chapter11.window;

import com.atguigu.flink.bean.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @Author: dsy
 * @Date: 2022/8/18 18:05
 * @Desciption:
 */


public class Flink01_Table_BaseUse {
    public static void main(String[] args) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", 1000L, 10),
                new WaterSensor("sensor_1", 2000L, 20),
                new WaterSensor("sensor_1", 3000L, 30),
                new WaterSensor("sensor_1", 4000L, 40),
                new WaterSensor("sensor_1", 5000L, 50),
                new WaterSensor("sensor_1", 6000L, 60)
        );

        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        //把流转换成表
        Table table = tEnv.fromDataStream(stream);
        table.printSchema();
        //在表上执行查询，得到一个新的动态表

        Table result = table
                .where("id='sensor_1'")
                .select("id, vc");
        //把动态表转成流
        //DataStream<WaterSensor> resultStream = tEnv.toDataStream(result, WaterSensor.class);
        //DataStream<Row> resultStream = tEnv.toDataStream(result, Row.class);
        DataStream<Row> resultStream = tEnv.toAppendStream(result, Row.class);

        //输出结果
        resultStream.print();

        try {
            env.execute();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
