package stu.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaDeserializationSchemaWrapper;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.source
 @Author: wujiafu
 @CreateTime: 2023-12-02  00:00
 @Description: kafka数据源示例
 @Version: 1.0 */
public class KafkaSourceDemo {

    public static void main(String[] args) throws Exception {

        //1.定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();


        KafkaSource<String> build = KafkaSource.<String>builder()
                //定义kafka集群地址
                .setBootstrapServers("192.168.129.159:9092,192.168.129.162:9092,192.168.129.163:9092")
                //设置要消费的主题
                .setTopics("kfk-tese1")
                //设置消费者组
                .setGroupId("test1")
                //设置消费者策略，1）从最早的位置消费，2）从最新的位置消费，3）从指定的位置开始消费
                .setStartingOffsets(OffsetsInitializer.earliest())
                //定义反序列化类
                .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(new SimpleStringSchema()))
                .build();

        // 添加数据源
        executionEnvironment.fromSource(build, WatermarkStrategy.noWatermarks(),"kafkaSource").print();
        //执行
        executionEnvironment.executeAsync();

    }
}