package stu.flink.source;

import org.apache.flink.api.common.eventtime.TimestampAssignerSupplier;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.source
 @Author: wujiafu
 @CreateTime: 2023-12-01  23:33
 @Description: 文件与套接字数据源示例
 @Version: 1.0 */
public class FileSourceDemo {
    public static void main(String[] args) throws Exception {

        //1.定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2.获取file数据源
        FileSource<String> fileSource = FileSource.forRecordStreamFormat(new TextLineInputFormat(), new Path("file/test.txt")).build();

        //2.获取socket数据流
        //executionEnvironment.socketTextStream("192.168.128.183",8086);
        // 添加数据源
        executionEnvironment.fromSource(fileSource,WatermarkStrategy.noWatermarks(),"myDataSource").print();


        //执行程序
        executionEnvironment.execute();

    }
}