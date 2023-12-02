package stu.flink.sink;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.BasePathBucketAssigner;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.sink
 @Author: wujiafu
 @CreateTime: 2023-12-02  23:23
 @Description: 文件输出流
 @Version: 1.0 */
public class FileSinkDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT,"8081");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置为批任务
        //executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //设置为流任务
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //设置为自动模式
        //executionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //executionEnvironment.
        //直接使用lambada会导致类型擦除，需添加returns(Types.TUPLE(Types.STRING,Types.INT))
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.socketTextStream("192.168.128.183", 9999);

        FileSink fileSink = FileSink.forRowFormat(new Path("file/hello.txt"),new SimpleStringEncoder()).build();

        //将流输出到文件
        stringDataStreamSource.sinkTo(fileSink);

        //执行
        executionEnvironment.execute();

    }
}