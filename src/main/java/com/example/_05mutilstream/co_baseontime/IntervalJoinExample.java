package com.example._05mutilstream.co_baseontime;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

/**
 * @Description 间隔联结示例
 * 为避免想要的数据分配到两个窗口造成无法计算；
 * 针对一条流中的每个数据开辟出其时间戳前后的一段时间间隔 [x.timestamp + 上界upperBound，x.timestamp + 下界lowerBound ]，
 * 看此段时间是否有另一条流同key的数据可以匹配： x.timestamp + lowerBound  <= y.timestamp <= x.timestamp + 上界upperBound（上下界可正可负）
 *
 * 示例：商城用户行为在短时间内有强关联时，如下单流、浏览数据流，两流中下单事件 与 前5s后10s 内浏览数据 的联合查询
 * @Author kerry
 * @Date 2024/4/25 11:35
 */
public class IntervalJoinExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String, String, Long>> orderStream = env.fromElements(
                Tuple3.of("Kerry", "order-1", 5000L),
                Tuple3.of("Johnson", "order-2", 5000L),
                Tuple3.of("Eli", "order-3", 20000L),
                Tuple3.of("Happy", "order-4", 20000L),
                Tuple3.of("Eli", "order-5", 51000L)
        ) .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (e, l) -> e.f2));

        SingleOutputStreamOperator<Tuple3<String, String, Long>> clickStream = env.fromElements(
                Tuple3.of("Kerry", "/cart", 2000L),
                Tuple3.of("Eli", "/prod?id=1", 3000L),
                Tuple3.of("Kerry", "/prod?id=2", 3500L),
                Tuple3.of("Eli", "/prod?id=200", 2500L),
                Tuple3.of("Eli", "/prod?id=3", 36000L),
                Tuple3.of("Kerry", "/prod?id=20", 30000L),
                Tuple3.of("Kerry", "/prod?id=4", 23000L),
                Tuple3.of("Kerry", "/home", 33000L)
        ) .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (e, l) -> e.f2));

        orderStream.keyBy(data -> data.f0)
                .intervalJoin(clickStream.keyBy(data -> data.f0))
                .between(Time.seconds(-5), Time.seconds(10))
                .process(new ProcessJoinFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>() {
                    @Override
                    public void processElement(Tuple3<String, String, Long> left, Tuple3<String, String, Long> right,Context context, Collector<String> collector) throws Exception {
                        collector.collect(right + "=>" + left);
                    }
                })
                .print();

        env.execute();
    }
}
