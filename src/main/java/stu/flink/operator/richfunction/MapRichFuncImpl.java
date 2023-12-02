package stu.flink.operator.richfunction;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.operator.richfunction
 @Author: wujiafu
 @CreateTime: 2023-12-02  16:14
 @Description: 带有生命周期的富函数转换算子
 @Version: 1.0 */
public class MapRichFuncImpl extends RichMapFunction<WaterSensor,String> {



    @Override
    public void open(Configuration parameters) throws Exception {
        RuntimeContext runtimeContext = getRuntimeContext();
        System.out.print("任务名称"+runtimeContext.getTaskName()+",任务id"+runtimeContext.getIndexOfThisSubtask());
        System.out.println("open方法开始调用了");

    }

    @Override
    public void close() throws Exception {
        //runtimeContext必须写在open或者close方法中才能获取到上下文
        RuntimeContext runtimeContext = getRuntimeContext();
        System.out.print("任务名称"+runtimeContext.getTaskName()+",任务id"+runtimeContext.getIndexOfThisSubtask());
        System.out.println("close方法开始调用了");
    }

    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.getId();
    }
}