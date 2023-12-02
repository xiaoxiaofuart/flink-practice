package stu.flink.operator.aggregate;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.operator.aggregate
 @Author: wujiafu
 @CreateTime: 2023-12-02  14:12
 @Description: keyby 聚合算子模型
 @Version: 1.0 */
public class KeyByOpreratorDemo {
    public static void main(String[] args) throws Exception {

        //定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        executionEnvironment.setParallelism(2);

        //定义数据源
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(
                new WaterSensor("ws1", 1L, 2),
                new WaterSensor("ws1", 5L, 3),
                new WaterSensor("ws1", 6L, 8),
                new WaterSensor("ws2", 1L, 20),
                new WaterSensor("ws2", 18L, 10),
                new WaterSensor("ws3", 1L, 5));


        //定义一个分区键，按照水位id进行分组
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorDataStreamSource.keyBy(waterSensor -> waterSensor.getId());

        //输出结果
        waterSensorStringKeyedStream.print();

        //执行结果
        executionEnvironment.execute();

    }
}