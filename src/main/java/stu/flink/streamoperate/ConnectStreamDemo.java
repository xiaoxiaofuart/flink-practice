package stu.flink.streamoperate;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.streamoperate
 @Author: wujiafu
 @CreateTime: 2023-12-02  21:40
 @Description: flink的流合并操作
 @Version: 1.0 */
public class ConnectStreamDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT,"8081");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 2, 3, 4);
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.fromElements("5", "6", "7", "8");

        ConnectedStreams<String,Integer > connect = stringDataStreamSource.connect(integerDataStreamSource);

        //connect流一次只能连接两个流，两个流的类型可以不一致，可以使用process算子将两个流处理成一个类型，合并成一个流，也可以使用map算子
        SingleOutputStreamOperator returns = connect.process((CoProcessFunction) new CoProcessFunction<String, Integer, String>() {
            @Override
            public void processElement1(String value, CoProcessFunction<String, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(value);
            }

            @Override
            public void processElement2(Integer value, CoProcessFunction<String, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                out.collect(String.valueOf(value));
            }
        }).returns(Types.STRING);


        returns.print();

        executionEnvironment.execute();

    }


}