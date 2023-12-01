package stu.flink.env;

import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.env
 @Author: wujiafu
 @CreateTime: 2023-12-01  23:19
 @Description: flink 执行环境说明
 @Version: 1.0 */
public class FlinkExcutionEnviromentDemo {
    public static void main(String[] args) {

        /*
         * 通过flink 程序自动推导获取执行环境，flink是通过configration对象进行判断
         * 1.首先找有没有远端执行环境，有的话，就生成远端执行环境，没有的话就生成一个本地的执行环境，日常开发按照此写法即可
         * */
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //2.手动获取一个远端的执行环境
        StreamExecutionEnvironment remoteEnvironment = StreamExecutionEnvironment.createRemoteEnvironment("192.168.128.183", 8086);


        //3.手动创建一个本地的执行环境
        LocalStreamEnvironment localEnvironment = StreamExecutionEnvironment.createLocalEnvironment();

    }
}