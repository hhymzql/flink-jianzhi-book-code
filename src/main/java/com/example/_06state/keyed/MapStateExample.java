package com.example._06state.keyed;

import com.example.util.pojo.WaterSensor;
import com.example.util.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Description 关于状态编程的代码示例:MapState
 * 模拟窗口的实现
 * @Author kerry
 * @Date 2024/4/24 13:29
 */
public class MapStateExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (water, l) -> water.time));

        // 统计每10s窗口内 ts 出现的频率
        stream.keyBy(data-> data.ts)
                .process(new FakeWindowResult(10000L))
                .print();

        env.execute();
    }

    /**
     * 1. 注册定时器，周期性输出pv
     */
    public static class FakeWindowResult extends KeyedProcessFunction<String, WaterSensor, String>{

        private Long windowSize;

        public FakeWindowResult(Long windowSize) {
            this.windowSize = windowSize;
        }

        // 需要将状态定义为类的属性，方便其他方法访问；但外部不能直接获取状态，故在外部声明状态对象，在open通过运行时上下文获取状态对象
        MapState<Long, Long> windowPvMapState;

        // open方法只会调用一次，故在open中获取状态对象
        @Override
        public void open(Configuration parameters){
            windowPvMapState = getRuntimeContext().getMapState(new MapStateDescriptor<>("window", Long.class, Long.class));
        }

        @Override
        public void processElement(WaterSensor value,Context context, Collector<String> collector) throws Exception {
            // 每来一条数据，就根据时间戳判断是哪个窗口
            long windowStart = value.time / windowSize * windowSize;
            long windowEnd = windowStart + windowSize;

            // 注册windowEnd - 1 的定时器，窗口触发计算
            context.timerService().registerProcessingTimeTimer(windowEnd - 1);

            // 更新状态中的pv值
            if(windowPvMapState.contains(windowStart)){
                Long pv = windowPvMapState.get(windowStart);
                windowPvMapState.put(windowStart, pv + 1);
            }else {
                windowPvMapState.put(windowStart, 1L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<String> collector) throws Exception {
            long windowEnd = timestamp + 1;
            long windowStart = windowEnd - windowSize;
            long pv = windowPvMapState.get(windowStart);
            collector.collect("ts:" + context.getCurrentKey() +
                    " 频率: " + pv +
                    " 窗口：" + new Timestamp(windowStart) + "~" + new Timestamp(windowEnd));

            // 模拟窗口销毁，清除map中的key
            windowPvMapState.clear();
        }
    }
}
