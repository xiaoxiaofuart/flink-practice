package stu.flink.wc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.stream.Collectors;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.wc
 @Author: wujiafu
 @CreateTime: 2023-11-30  15:38
 @Description: stream api
 @Version: 1.0 */
public class FlinkSocketWC {

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
        executionEnvironment.socketTextStream("192.168.128.183",9999).flatMap(
                (FlatMapFunction<String, Tuple2<String, Integer>>) (s, collector) -> {
                    for (String s1 : s.split(",")) {
                        collector.collect(Tuple2.of(s1,1));
                    }

                }
        ).returns(Types.TUPLE(Types.STRING,Types.INT)).keyBy(0).sum(1).print();

        executionEnvironment.execute();
    }

}