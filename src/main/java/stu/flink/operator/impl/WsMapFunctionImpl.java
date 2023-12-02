package stu.flink.operator.impl;

import org.apache.flink.api.common.functions.MapFunction;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.operator.impl
 @Author: wujiafu
 @CreateTime: 2023-12-02  13:00
 @Description: 定义一个watersensor的mapfunction
 @Version: 1.0 */
public class WsMapFunctionImpl implements MapFunction<WaterSensor,String> {

    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.getId();
    }
}