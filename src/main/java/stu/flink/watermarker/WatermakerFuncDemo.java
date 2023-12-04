package stu.flink.watermarker;

import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.window
 @Author: wujiafu
 @CreateTime: 2023-12-03  13:15
 @Description: 全聚合函数
 @Version: 1.0 */
public class WatermakerFuncDemo {

    public static void main(String[] args) throws Exception {
        //定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.setParallelism(1);


        //定义数据源
        SingleOutputStreamOperator<WaterSensor> stringDataStreamSource = executionEnvironment.socketTextStream("192.168.128.183", 9999).map(str -> {
            String[] split = str.split(",");
            WaterSensor waterSensor = new WaterSensor();
            waterSensor.setId(split[0]);
            waterSensor.setTs(Long.valueOf(split[1]));
            waterSensor.setVc(Integer.valueOf(split[2]));
            return waterSensor;
        });


        //定义水位线
        SingleOutputStreamOperator<WaterSensor> waterSensorSingleOutputStreamOperator = stringDataStreamSource
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps().withTimestampAssigner(new SerializableTimestampAssigner<WaterSensor>() {
                    @Override
                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                        return element.getTs()*1000;
                    }
                }));



        //按照水位线id对数据进行分流
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorSingleOutputStreamOperator.keyBy(str -> str.getId());
        //获取窗口流,需要将processtime 改为 eventtime
        WindowedStream<WaterSensor, String, TimeWindow> windowedStream = waterSensorStringKeyedStream.window(TumblingEventTimeWindows.of(Time.seconds(10)));


        /*
         * 使用process函数使得聚合函数处理更加灵活，可以通过上下文获取窗口的一些信息
         * 同时也可以在窗口结束时获取接收到的结果集
        **/
        SingleOutputStreamOperator<String> process = windowedStream.process(new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
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