package stu.flink.streamoperate;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;
import stu.flink.vo.WaterSensor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.streamoperate
 @Author: wujiafu
 @CreateTime: 2023-12-02  21:40
 @Description: flink的流合并操作
 @Version: 1.0 */
public class StreamJoinDemo {

    public static void main(String[] args) throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(RestOptions.BIND_PORT,"8081");

        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> integerDataStreamSource = executionEnvironment.fromElements(new WaterSensor("ws1", 1L, 2),
                new WaterSensor("ws1", 5L, 3),
                new WaterSensor("ws1", 6L, 8),
                new WaterSensor("ws2", 1L, 20),
                new WaterSensor("ws2", 18L, 10),
                new WaterSensor("ws3", 1L, 5));

        //定义一个map用于存储第一个流中的数据
        Map<String, List<WaterSensor>> streamMap1 = new HashMap<>(16);

        //定义一个map用于存储第二个流中的数据
        Map<String, List<WaterSensor>> streamMap2 = new HashMap<>(16);

        DataStreamSource<WaterSensor> integerDataStreamSource1 = executionEnvironment.fromElements(
                new WaterSensor("ws1", 10L, 3),
                new WaterSensor("ws2", 5L, 12));

        //connect流一次只能连接两个流，两个流的类型可以不一致，可以使用process算子将两个流处理成一个类型，合并成一个流，也可以使用map算子
        ConnectedStreams<WaterSensor,WaterSensor > connect = integerDataStreamSource.connect(integerDataStreamSource1);

        //指定两个流中的分区策略，这个必须保持一致，否则会导致两个流相同key分配不到同一个分区，导致合并的流数据丢失
        ConnectedStreams<WaterSensor, WaterSensor> waterSensorWaterSensorConnectedStreams = connect.keyBy(waterSensor -> waterSensor.getId(), waterSensor1 -> waterSensor1.getId());

        //对两个流进行判断，是否存在相同key，存在则输出合并流
        SingleOutputStreamOperator<String> returns = waterSensorWaterSensorConnectedStreams.process(new CoProcessFunction<WaterSensor, WaterSensor, String>() {
            @Override
            public void processElement1(WaterSensor value, CoProcessFunction<WaterSensor, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                //判断数据集中书否有，没有则放进map,方便后续的数据join
                if (streamMap1.containsKey(value.getId())) {
                    streamMap1.get(value.getId()).add(value);
                } else {
                    ArrayList<WaterSensor> arrayList = new ArrayList<>(16);
                    arrayList.add(value);
                    streamMap1.put(value.getId(), arrayList);
                }

                //判断流2中是否能否匹配数据，能匹配则输出结果到合并流中
                if (streamMap2.containsKey(value.getId())) {
                    for (WaterSensor waterSensor : streamMap2.get(value.getId())) {
                        out.collect(waterSensor + "=======>输出流1 " + value);
                    }
                }

            }

            @Override
            public void processElement2(WaterSensor value, CoProcessFunction<WaterSensor, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                //判断数据集中书否有，没有则放进map,方便后续的数据join
                if (streamMap2.containsKey(value.getId())) {
                    streamMap2.get(value.getId()).add(value);
                } else {
                    ArrayList<WaterSensor> arrayList = new ArrayList<>(16);
                    arrayList.add(value);
                    streamMap2.put(value.getId(), arrayList);
                }

                //判断流1中是否能否匹配数据，能匹配则输出结果到合并流中
                if (streamMap1.containsKey(value.getId())) {
                    for (WaterSensor waterSensor : streamMap1.get(value.getId())) {
                        out.collect(waterSensor + "=======>输出流2 " + value);
                    }
                }
            }
        }).returns(Types.STRING);

        //输出流
        returns.print();

        executionEnvironment.execute();

    }


}