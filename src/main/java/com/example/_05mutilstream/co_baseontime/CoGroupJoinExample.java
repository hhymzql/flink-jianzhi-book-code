package com.example._05mutilstream.co_baseontime;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description 窗口同组联结示例
 * 与窗口联结类似，但更通用，不只是内连接，还可实现左外连接、右外连接、全外连接（窗口联结底层是通过同组联结实现的）coGroup只会被调用一次
 * @Author kerry
 * @Date 2024/4/25 15:05
 */
public class CoGroupJoinExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Long>> stream1 = env.fromElements(
                Tuple2.of("a", 5500L),
                Tuple2.of("a", 6000L),
                Tuple2.of("b", 9000L),
                Tuple2.of("a", 9000L),
                Tuple2.of("b", 12000L)
        ) .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (e, l) -> e.f1));

        SingleOutputStreamOperator<Tuple2<String, Long>> stream2 = env.fromElements(
                Tuple2.of("a", 9999L),
                Tuple2.of("b", 13000L),
                Tuple2.of("a", 14000L),
                Tuple2.of("b", 14000L),
                Tuple2.of("a", 14999L)
        ) .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple2<String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple2<String, Long>>) (e, l) -> e.f1));

        stream1.coGroup(stream2)
                .where(data -> data.f0)
                .equalTo(data -> data.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple2<String, Long>, Tuple2<String, Long>, String>(){
                    @Override
                    public void coGroup(Iterable<Tuple2<String, Long>> iter1, Iterable<Tuple2<String, Long>> iter2, Collector<String> collector) throws Exception {
                        collector.collect(iter1 + "=>" + iter2);
                    }
                })
                .print();

        env.execute();

    }
}
