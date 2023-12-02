package stu.flink.operator.transform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import stu.flink.vo.WaterSensor;

import java.util.ArrayList;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.operator.transform
 @Author: wujiafu
 @CreateTime: 2023-12-02  13:19
 @Description: filter过滤器算子
 @Version: 1.0 */
public class FilterOperatorDemo {

    public static void main(String[] args) throws Exception {
        //定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        ArrayList<WaterSensor>  waterSensors = new ArrayList<>(16);
        waterSensors.add(new WaterSensor("ws1", 1L, 8));
        waterSensors.add(new WaterSensor("ws2", 1L, 8));
        waterSensors.add(new WaterSensor("ws2", 2L, 15));
        waterSensors.add(new WaterSensor("ws3", 1L, 1));
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromCollection(waterSensors);

        //定义filter算子，id等于ws2才输出日志
        waterSensorDataStreamSource.filter(waterSensor -> "ws2".equals(waterSensor.getId())).print();


        //执行
        executionEnvironment.execute();
    }
}