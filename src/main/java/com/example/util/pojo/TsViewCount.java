package com.example.util.pojo;

import java.sql.Timestamp;

/**
 * @Description 增量与全窗口函数结合使用时表示聚合输出结果的数据类
 * @Author kerry
 * @Date 2024/4/12 15:21
 */
public class TsViewCount {

    public String ts;
    public Long count;
    public Long windowStart;
    public Long windowEnd;

    public TsViewCount(){}

    public TsViewCount(String ts, Long count, Long windowStart, Long windowEnd) {
        this.ts = ts;
        this.count = count;
        this.windowStart = windowStart;
        this.windowEnd = windowEnd;
    }

    @Override
    public String toString() {
        return "TsViewCount{" +
                "ts='" + ts + '\'' +
                ", count=" + count +
                ", windowStart=" + new Timestamp(windowStart) +
                ", windowEnd=" + new Timestamp(windowEnd) +
                '}';
    }
}
