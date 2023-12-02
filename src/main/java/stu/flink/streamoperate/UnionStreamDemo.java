package stu.flink.streamoperate;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.streamoperate
 @Author: wujiafu
 @CreateTime: 2023-12-02  21:40
 @Description: flink的流合并操作
 @Version: 1.0 */
public class UnionStreamDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT,"8081");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<Integer> integerDataStreamSource = executionEnvironment.fromElements(1, 2, 3, 4);
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.fromElements("5", "6", "7", "8");

        //使用union合并两个流，要求两个流的类型必须一致，否则不允许
        DataStream<Integer> union = integerDataStreamSource.union(stringDataStreamSource.map(str -> Integer.valueOf(str)));

        union.print();

        executionEnvironment.execute();

    }


}