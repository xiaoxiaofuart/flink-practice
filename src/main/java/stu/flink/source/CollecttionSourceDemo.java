package stu.flink.source;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.source
 @Author: wujiafu
 @CreateTime: 2023-12-01  23:26
 @Description: 从集合中加载source数据源示例
 @Version: 1.0 */
public class CollecttionSourceDemo {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //1.从元素中获取数据源
       //executionEnvironment.fromElements(1, 2, 3, 4).print();

        //2.从集合中加载数据源
       executionEnvironment.fromCollection(Arrays.asList(1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17)).print();


         //3.总结，以上两种方式主要用来进行日常数据测试
        executionEnvironment.execute();

    }
}