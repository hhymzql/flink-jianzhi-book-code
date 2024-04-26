package com.example._03time_and_window;

import com.example.util.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description 测试水位线和窗口的使用，展示当前水位线和窗口信息
 * @Author kerry
 * @Date 2024/4/12 15:47
 */
public class WatermarkAndWindowTest {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        env.socketTextStream("127.0.0.1", 7777)
                .map((MapFunction<String, WaterSensor>) s -> {
                    String[] input = s.split(",");
                    return new WaterSensor(input[0].trim(), input[1].trim(), Integer.parseInt(input[2].trim()));
                })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (sensor, l) -> sensor.getTime()))
                .keyBy(data -> data.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .process(new WatermarkTestResult())
                .print();

        env.execute();
    }

    public static class WatermarkTestResult extends ProcessWindowFunction<WaterSensor, String, String, TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> ele, Collector<String> collector) throws Exception {
            long start = context.window().getStart();
            long end = context.window().getEnd();
            long currWatermark = context.currentWatermark();
            // 获取迭代器中元素个数
            long count = ele.spliterator().getExactSizeIfKnown();
            collector.collect("窗口：" + start + "~" + end + "共用" + count + "个元素，窗口闭合计算时的水位线处于：" + currWatermark);
        }
    }

}
