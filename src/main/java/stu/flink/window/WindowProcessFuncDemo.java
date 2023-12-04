package stu.flink.window;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.window
 @Author: wujiafu
 @CreateTime: 2023-12-03  13:15
 @Description: 全聚合函数
 @Version: 1.0 */
public class WindowProcessFuncDemo {

    public static void main(String[] args) throws Exception {
        //定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义数据源
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.socketTextStream("192.168.128.183", 9999);

        //按照水位线id对数据进行分流
        KeyedStream<String, String> waterSensorStringKeyedStream = stringDataStreamSource.keyBy(str -> str.split(",")[0]);

        //获取窗口流
        WindowedStream<String, String, TimeWindow> windowedStream = waterSensorStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(30)));


        /*
         * 使用process函数使得聚合函数处理更加灵活，可以通过上下文获取窗口的一些信息
         * 同时也可以在窗口结束时获取接收到的结果集
        **/
        SingleOutputStreamOperator<String> process = windowedStream.process(new ProcessWindowFunction<String, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {
                long start = context.window().getStart();
                long end = context.window().getEnd();
                String startDate = DateFormatUtils.format(start, "yyyy-mm-dd HH:mm:ss:SSS");
                String endDate = DateFormatUtils.format(end, "yyyy-mm-dd HH:mm:ss:SSS");

                out.collect("窗口开始时间：" + startDate + "窗口结束时间：" + endDate + "一共接收到了" + elements.spliterator().estimateSize() + "条数据，数据内容为" + elements.toString());

            }
        });


        process.print();

        executionEnvironment.execute();
    }
}