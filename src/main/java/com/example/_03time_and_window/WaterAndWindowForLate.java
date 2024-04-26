package com.example._03time_and_window;

import com.example.util.pojo.TsViewCount;
import com.example.util.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * @Description 处理迟到数据的三步走
 * @Author kerry
 * @Date 2024/4/15 13:54
 */
public class WaterAndWindowForLate {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env.socketTextStream("127.0.0.1", 7777)
                .map((MapFunction<String, WaterSensor>) s -> {
                    String[] input = s.split(",");
                    return new WaterSensor(input[0].trim(), input[1].trim(), Integer.parseInt(input[2].trim()));
                })
                // 方式一：设置水位线延迟时间
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (sensor, l) -> sensor.getTime()));

        OutputTag outputTag = new OutputTag<WaterSensor>("late"){};
        SingleOutputStreamOperator<TsViewCount> aggregate = stream.keyBy(data -> data.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 方式二：允许窗口延迟关闭的等待时间
                .allowedLateness(Time.minutes(1))
                // 方式三：将迟到数据放入侧输出流
                .sideOutputLateData(outputTag)
                .aggregate(new WindowExample.AvgPv());
        SideOutputDataStream sideOutput = aggregate.getSideOutput(outputTag);

        aggregate.print("result:");
        sideOutput.print("late");

        env.execute();
    }
}
