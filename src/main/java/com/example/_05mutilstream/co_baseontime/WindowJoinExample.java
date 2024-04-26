package com.example._05mutilstream.co_baseontime;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @Description 窗口联结使用示例：
 * 定义时间窗口并将共享key的数据在窗口中进行配对处理（类似：select * from t1 , t2 where t1.id = t2.id）
 * @Author kerry
 * @Date 2024/4/25 10:37
 */
public class WindowJoinExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 1000L),
                Tuple2.of("b", 1000L),
                Tuple2.of("a", 2000L),
                Tuple2.of("b", 2000L)
        ) .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (e, l) -> e.f1));

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env.fromElements(
                Tuple2.of("a", 3000L),
                Tuple2.of("b", 3000L),
                Tuple2.of("a", 4000L),
                Tuple2.of("b", 4000L)
        ) .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (e, l) -> e.f1));

        stream1.join(stream2)
                .where(data -> data.f0)
                .equalTo(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply((JoinFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>) (left, right) -> left + "=>" + right)
                .print();

        env.execute();

    }
}
