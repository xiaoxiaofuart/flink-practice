package stu.flink.watermarker;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.watermarker
 @Author: wujiafu
 @CreateTime: 2023-12-04  16:49
 @Description: 窗口双流join
 @Version: 1.0 */
public class IntervalStreamJoinDemo {


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

        SingleOutputStreamOperator<String> process = tuple2StringKeyedStream
                .intervalJoin(tuple3StringKeyedStream)
                //定义间隔
                .between(Time.seconds(-2), Time.seconds(2))
                .process(new ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                    @Override
                    public void processElement(Tuple2<String, Integer> left, Tuple3<String, Integer, Integer> right, ProcessJoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left.toString() + "==========" + right.toString());
                    }
                });


        process.print();

        executionEnvironment.execute();

    }
}