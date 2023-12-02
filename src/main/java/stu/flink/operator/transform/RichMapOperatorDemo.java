package stu.flink.operator.transform;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import stu.flink.operator.impl.WsMapFunctionImpl;
import stu.flink.operator.richfunction.MapRichFuncImpl;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.operator.transform
 @Author: wujiafu
 @CreateTime: 2023-12-02  12:46
 @Description: map转换算子实例
 @Version: 1.0 */
public class RichMapOperatorDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(new WaterSensor("ws1", 1L, 2),
                new WaterSensor("ws2", 1L, 8),
                new WaterSensor("ws3", 1L, 5));

        //带有生命周期富函数的转换算子，open方法在项目启动时就会调用，close方法在程序运行结束或者手动cancle时调用，异常终止的程序不会调用close方法
        waterSensorDataStreamSource.map(new MapRichFuncImpl()).print();

        executionEnvironment.execute();
    }
}