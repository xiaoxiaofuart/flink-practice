package stu.flink.datatype;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.datatype
 @Author: wujiafu
 @CreateTime: 2023-12-02  11:39
 @Description: flink中的类型说明
 @Version: 1.0 */
public class FlinkTypeInfo {
    public static void main(String[] args) {
        //1.flink中内置了一个序列化类型TypeInformation，底层是使用kyro实现的序列化,flink中所有类型都是基于此类型进行操作的
        //2.flink 提供了Types类，帮助研发人员更好的使用flink中的类型,比如：Types.TUPLE(Types.STRING,Types.INT)
        Types.TUPLE(Types.STRING,Types.INT);
        //3.flink 提供了一个类型提取器，研发人员可以借助此类型自己实现一个类型
        TypeHint<String> typeHint = new TypeHint<String>() {};

        //4.flink中使用自定的pojo一些约束1）类要是public修饰的2）成员变量要可访问3）字段类型都是可序列化的4）有一个无参的构造方法

    }
}