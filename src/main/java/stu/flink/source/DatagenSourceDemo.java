package stu.flink.source;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.api.java.typeutils.GenericTypeInfo;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.datagen.DataGenerator;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.source
 @Author: wujiafu
 @CreateTime: 2023-12-02  00:39
 @Description: 数据生成器source示例
 @Version: 1.0 */
public class DatagenSourceDemo {
    public static void main(String[] args) throws Exception {

        //定义执行环境
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //定义datagen数据源
        DataGeneratorSource<Long> longDataGeneratorSource = new DataGeneratorSource<>((GeneratorFunction<Long, Long>) aLong -> aLong, 100, RateLimiterStrategy.perSecond(5),GenericTypeInfo.of(Long.class));

        //添加数据源
        executionEnvironment.fromSource(longDataGeneratorSource, WatermarkStrategy.noWatermarks(),"datagenSource").print();

        //执行
        executionEnvironment.executeAsync();

    }
}