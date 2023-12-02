package stu.flink.streamoperate;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.streamoperate
 @Author: wujiafu
 @CreateTime: 2023-12-02  23:02
 @Description: 侧边输出流
 @Version: 1.0 */
public class SideStreamDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT,"8081");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> integerDataStreamSource = executionEnvironment.fromElements(new WaterSensor("ws1", 1L, 2),
                new WaterSensor("ws1", 5L, 3),
                new WaterSensor("ws1", 6L, 8),
                new WaterSensor("ws2", 1L, 20),
                new WaterSensor("ws2", 18L, 10),
                new WaterSensor("ws3", 1L, 5));

        //定义一个侧边输出标签
       final OutputTag<WaterSensor> outputTag = new OutputTag<>("ws1", TypeInformation.of(WaterSensor.class));

        //定义一个底层处理算子，在主流程中将id为ws1的数据使output函数将数据输出到侧边流
        SingleOutputStreamOperator<WaterSensor> process = integerDataStreamSource.process(new ProcessFunction<WaterSensor, WaterSensor>() {
            @Override
            public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                if ("ws1".equals(value.getId())) {
                    //第一个参数为输出流格式，第二个参数为输出的值类型
                    ctx.output(outputTag, value);
                }
                out.collect(value);
            }
        });

        //获取侧边流
        SideOutputDataStream<WaterSensor> sideOutput = process.getSideOutput(outputTag);
        sideOutput.print("我是侧边输出");
        process.print("我是主流程输出");

        executionEnvironment.execute();
    }
}