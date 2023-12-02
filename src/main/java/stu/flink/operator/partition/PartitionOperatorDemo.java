package stu.flink.operator.partition;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.operator.partition
 @Author: wujiafu
 @CreateTime: 2023-12-02  19:25
 @Description: flink的分区配置
 @Version: 1.0 */
public class PartitionOperatorDemo {
    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT,"8081");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment(configuration);

        executionEnvironment.setParallelism(2);

        executionEnvironment.setRuntimeMode(RuntimeExecutionMode.STREAMING);


        //直接使用lambada会导致类型擦除，需添加returns(Types.TUPLE(Types.STRING,Types.INT))
        DataStreamSource<String> stringDataStreamSource = executionEnvironment.socketTextStream("192.168.128.183", 9999);


        //随机分区
        //stringDataStreamSource.shuffle().print();

        //轮询
        //stringDataStreamSource.rebalance().print();

        // 按照分组进行轮询,比如有多个taskmanager，每个taskmanager上面有5个任务
        //stringDataStreamSource.rescale().print();

        //广播发送分区，所有分区都能收到
        //stringDataStreamSource.broadcast().print();

        //数据都会送往第一个分区
        //stringDataStreamSource.global().print();

        //一对一
        //stringDataStreamSource.forward().print();

        //按照key进行分区
        //stringDataStreamSource.keyBy(s->s);

        //自定义分区,第一个参数是表示的分区策略，第一个key表示的分区key，第二个partionNum表示并行度，第二个参数str表示的是收到的元素
        stringDataStreamSource.partitionCustom((key,partionNum)-> key%partionNum, str -> Integer.valueOf(str)).print();

        executionEnvironment.execute();

    }
}