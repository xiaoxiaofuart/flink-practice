package stu.flink.operator.aggregate;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import stu.flink.vo.WaterSensor;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.operator.aggregate
 @Author: wujiafu
 @CreateTime: 2023-12-02  14:12
 @Description: sum、min、max、minby、maxby 算子
 @Version: 1.0 */
public class MinAndMaxOpreratorDemo {
    public static void main(String[] args) throws Exception {

        //定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置并行度
        executionEnvironment.setParallelism(1);

        //定义数据源
        DataStreamSource<WaterSensor> waterSensorDataStreamSource = executionEnvironment.fromElements(
                new WaterSensor("ws1", 1L, 2),
                new WaterSensor("ws1", 5L, 3),
                new WaterSensor("ws1", 6L, 8),
                new WaterSensor("ws2", 1L, 20),
                new WaterSensor("ws2", 18L, 10),
                new WaterSensor("ws3", 1L, 5));


        //定义一个分区键，按照水位id进行分组
        KeyedStream<WaterSensor, String> waterSensorStringKeyedStream = waterSensorDataStreamSource
                .keyBy(waterSensor -> waterSensor.getId());

        // sum、min、max、minby、maxby算子只能在分区后的KeyedSream 下面使用

        //sum方法可以接收两种参数，一种是pojo的字段名，还有一种是turple的索引,这里面的WaterSensor一定要加无参的构造函数，否则会报错
       // SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = waterSensorStringKeyedStream.sum("vc");

        //min与minby的区别，min中输出的其他字段只会取第一次的记录，后面不会更新，minby会取最大值对应的那条记录的其他数据
       // SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = waterSensorStringKeyedStream.min("vc");
        //SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = waterSensorStringKeyedStream.minBy("vc");


        // max与maxby的区别与上面min同理
        SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = waterSensorStringKeyedStream.max("vc");
        //SingleOutputStreamOperator<WaterSensor> singleOutputStreamOperator = waterSensorStringKeyedStream.maxBy("vc");


        //输出结果
        singleOutputStreamOperator.print();

        //执行结果
        executionEnvironment.execute();

    }
}