package stu.flink.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.window
 @Author: wujiafu
 @CreateTime: 2023-12-03  13:15
 @Description: 窗口函数的定义与使用
 @Version: 1.0 */
public class WindowDefineDemo {

    public static void main(String[] args) throws Exception {
        //定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义数据源
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(new WaterSensor("ws1", 1L, 500),
                new WaterSensor("ws1", 50L, 100),
                new WaterSensor("ws2", 80L, 600),
                new WaterSensor("ws3", 800L, 700)
        );

        //按照水位线id对数据进行分流
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorDataStreamSource.keyBy(waterSensor -> waterSensor.getId());


        //不经过keyby的流使用windowall后，数据不能分流，智能在一条流里面，也就是并行度只能为1
        //waterSensorDataStreamSource.windowAll(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)));

        //经过keyby后的流会分流，按照分区，每个分区会创建自己的单独的window进行计数
        //waterSensorStringKeyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10),Time.seconds(2)));

        //创建滚动窗口,使用TumblingProcessingTimeWindows.of创建一个滚动窗口，滚动间隔为10秒
        //waterSensorStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        //创建滑动窗口,使用SlidingProcessingTimeWindows.of创建一个滑动窗口，窗口大小为10秒，每隔两秒创建一个
        waterSensorStringKeyedStream.window(SlidingProcessingTimeWindows.of(Time.seconds(10), Time.seconds(2)));

        //创建一个窗口大小为5个的滚动窗口
        //waterSensorStringKeyedStream.countWindow(5);

        //创建一个窗口大小为5个的滑动窗口，每达到两个元素窗口创建一个滑动窗口
        //waterSensorStringKeyedStream.countWindow(5,2);

        //创建一个全局窗口
        //waterSensorStringKeyedStream.window(GlobalWindows.create());

        //会话窗口
        waterSensorStringKeyedStream.window(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        executionEnvironment.execute();
    }
}