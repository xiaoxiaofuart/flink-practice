package stu.flink.watermarker;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssigner;
import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.time.Duration;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.watermarker
 @Author: wujiafu
 @CreateTime: 2023-12-04  16:49
 @Description: 窗口双流join
 @Version: 1.0 */
public class WindowStreamUnionDemo {


    public static void main(String[] args) throws Exception {

        //定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义并行度为5
        executionEnvironment.setParallelism(5);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tuple2SingleOutputStreamOperator = executionEnvironment.fromElements(Tuple2.of("a", 1),
                Tuple2.of("a", 15),
                Tuple2.of("b", 5),
                Tuple2.of("c", 6)
        ).assignTimestampsAndWatermarks(WatermarkStrategy
                .<Tuple2<String, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple2<String, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple2<String, Integer> element, long recordTimestamp) {
                        //此处注意返回的单位为毫秒
                        return element.f1*1000;
                    }
                }));


        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> tuple3SingleOutputStreamOperator = executionEnvironment.fromElements(Tuple3.of("a", 5, 15),
                Tuple3.of("a", 8, 8),
                Tuple3.of("a", 11, 8),
                Tuple3.of("b", 6, 6),
                Tuple3.of("c", 6, 9)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, Integer, Integer>>forMonotonousTimestamps()
                .withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<String, Integer, Integer>>() {
                    @Override
                    public long extractTimestamp(Tuple3<String, Integer, Integer> element, long recordTimestamp) {
                        return element.f1*1000;
                    }
                }));


        //将两个流按照元组第一个字段分区
        KeyedStream<Tuple2<String, Integer>, String> tuple2StringKeyedStream = tuple2SingleOutputStreamOperator.keyBy(tuple2 -> tuple2.f0);

        KeyedStream<Tuple3<String, Integer, Integer>, String> tuple3StringKeyedStream = tuple3SingleOutputStreamOperator.keyBy(tuple3 -> tuple3.f0);

        DataStream<String> apply = tuple2StringKeyedStream.join(tuple3StringKeyedStream)
                .where(tuple -> tuple.f0)
                .equalTo(tuple3 -> tuple3.f0)
                //使用滚动窗口，窗口大小为10秒
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))

                .apply(new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                        return first.toString() + "<tuple2===========tuple3>" + second.toString();
                    }
                });


        apply.print();

        executionEnvironment.execute();

    }
}