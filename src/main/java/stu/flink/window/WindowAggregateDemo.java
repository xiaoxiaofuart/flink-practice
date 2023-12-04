package stu.flink.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.window
 @Author: wujiafu
 @CreateTime: 2023-12-03  13:15
 @Description: 窗口聚合函数
 @Version: 1.0 */
public class WindowAggregateDemo {

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
         * 使用AggregateFunction构建一个聚合算子，方法传递三个类型
         * 第一个类型，表示输出类型
         * 第二个类型表示累加器存放的结果
         * 第三个参数表示输出的类型
         * 与reduce的区别是reduce要求输入与输出类型必须一致，aggregate则不需要
        **/
        SingleOutputStreamOperator<String> aggregate = windowedStream.aggregate(new AggregateFunction<String, Integer, String>() {
            @Override
            public Integer createAccumulator() {
                //创建一个累计器，并设置初始值
                return 0;
            }

            @Override
            public Integer add(String s, Integer integer) {
                //累加器运算逻辑
                System.out.println("我是累加器计算逻辑");
                integer = integer + Integer.valueOf(s.split(",")[2]);
                return integer;
            }

            @Override
            public String getResult(Integer integer) {
                //窗口结束时调用此方法
                System.out.println("我是窗口时间结束时调用");
                return "最终结果：" + integer.toString();
            }

            @Override
            public Integer merge(Integer integer, Integer acc1) {
                //会话窗口使用，暂时不管
                return null;
            }
        });


        aggregate.print();

        executionEnvironment.execute();
    }
}