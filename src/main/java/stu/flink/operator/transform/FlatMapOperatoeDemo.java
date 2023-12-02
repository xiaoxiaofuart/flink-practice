package stu.flink.operator.transform;

import org.apache.commons.collections.set.TypedSet;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import stu.flink.operator.impl.WsMapFunctionImpl;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.operator.transform
 @Author: wujiafu
 @CreateTime: 2023-12-02  13:35
 @Description: TODO
 @Version: 1.0 */
public class FlatMapOperatoeDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(new WaterSensor("ws1", 1L, 2),
                new WaterSensor("ws2", 1L, 8),
                new WaterSensor("ws3", 1L, 5));

        //定义一个flatmap 实现当水位记录为ws1时，只输出水位id，当水位记录为ws2时，输出水位id与水位值，当水位记录为ws3时，不输出结果
        waterSensorDataStreamSource.flatMap((FlatMapFunction<WaterSensor, String>) (waterSensor, collector) -> {
            if("ws1".equals(waterSensor.getId())){
                collector.collect(waterSensor.getId());
            }else if("ws2".equals(waterSensor.getId())){
                collector.collect(waterSensor.getId());
                collector.collect(waterSensor.getVc().toString());
            }
        }).returns(Types.STRING).print();

        executionEnvironment.execute();
    }

}