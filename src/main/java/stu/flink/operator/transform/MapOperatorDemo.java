package stu.flink.operator.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import stu.flink.operator.impl.WsMapFunctionImpl;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.operator.transform
 @Author: wujiafu
 @CreateTime: 2023-12-02  12:46
 @Description: map转换算子实例
 @Version: 1.0 */
public class MapOperatorDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(new WaterSensor("ws1", 1L, 2),
                new WaterSensor("ws2", 1L, 8),
                new WaterSensor("ws3", 1L, 5));

        //第一种方式，通过匿名内部类实现map算子
        waterSensorDataStreamSource.map(new MapFunction<WaterSensor, String>() {
            @Override
            public String map(WaterSensor waterSensor) throws Exception {
                return waterSensor.getId();
            }
        }).print();


        //第二中方式，通过labada表达式实现map 算子
        waterSensorDataStreamSource.map(waterSensor ->  waterSensor.getId()).print();

        //第三种方式，通过实现MapFunction 接口实现算子
        waterSensorDataStreamSource.map(new WsMapFunctionImpl()).print();

        executionEnvironment.execute();
    }
}