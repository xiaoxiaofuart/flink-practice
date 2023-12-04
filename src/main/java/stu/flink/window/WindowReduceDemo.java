package stu.flink.window;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.window
 @Author: wujiafu
 @CreateTime: 2023-12-03  13:15
 @Description: 窗口函数的定义与使用
 @Version: 1.0 */
public class WindowReduceDemo {

    public static void main(String[] args) throws Exception {
        //定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义数据源
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.socketTextStream("192.168.128.183", 9999);

        //按照水位线id对数据进行分流
        KeyedStream<String, String> waterSensorStringKeyedStream = stringDataStreamSource.keyBy(str -> str.split(",")[0]);

        //获取窗口流
        WindowedStream<String, String, TimeWindow> windowedStream = waterSensorStringKeyedStream.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        /*
         * 1.定义reduce函数,reduce函数要求数据流的数据类型不能边
         * 2.只有在窗口结束时才会输出reduce结果
         * 3.只有接收到数据时才会输出
        **/
        SingleOutputStreamOperator<String> reduce = windowedStream.reduce((str1, str2) -> {
            System.out.println(str1 + "s1<======>s2" + str2);
            String[] streams1 = str1.split(",");
            String[] streams2 = str2.split(",");

            return "[" + streams1[0] + "," + streams1[1] + "," + String.valueOf(Integer.valueOf(streams1[2]) + Integer.valueOf(streams2[2]))+"]";
        });


        reduce.print();

        executionEnvironment.execute();
    }
}