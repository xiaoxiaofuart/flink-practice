package stu.flink.wc;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.wc
 @Author: wujiafu
 @CreateTime: 2023-11-30  15:38
 @Description: stream api
 @Version: 1.0 */
public class FlinkStreamWC {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT,"8081");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置为批任务
        //executionEnvironment.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //设置为流任务
        //executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);
        //设置为自动模式
        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.AUTOMATIC);
        //executionEnvironment.
        executionEnvironment.readTextFile("file/test.txt").flatMap(
               new FlatMapFunction<String,Tuple2<String ,Integer>>(){
                   @Override
                   public void flatMap(String str, Collector<Tuple2<String ,Integer>> collector) throws Exception {

                       for (String s : str.split(" ")) {
                           collector.collect(Tuple2.of(s,1));
                       }
                       //collector.collect(str.split(" "));
                   }
               }).keyBy(0).sum(1).print();

        executionEnvironment.execute();
    }

}