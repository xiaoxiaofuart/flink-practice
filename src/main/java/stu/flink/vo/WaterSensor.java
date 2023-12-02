package stu.flink.vo;

import java.util.Objects;

/**
 @BelongsProject: flink-practice
 @BelongsPackage: stu.flink.vo
 @Author: wujiafu
 @CreateTime: 2023-12-01  23:04
 @Description: 水位记录实体
 @Version: 1.0 */
public class WaterSensor {

        /*
         * 水位线id
         * */
        private String id;

         /*
        * 水位线记录时间戳
         * */
        private Long ts;

        /*
         * 水位值
         * */
        private Integer vc;

    public WaterSensor() {
    }

    public WaterSensor(String id, Long ts, Integer vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public Integer getVc() {
        return vc;
    }

    public void setVc(Integer vc) {
        this.vc = vc;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        WaterSensor that = (WaterSensor) o;
        return Objects.equals(id, that.id) && Objects.equals(ts, that.ts) && Objects.equals(vc, that.vc);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, ts, vc);
    }


    @Override
    public String toString() {
        return "WaterSensor{" +
                "id='" + id + '\'' +
                ", ts=" + ts +
                ", vc=" + vc +
                '}';
    }
}