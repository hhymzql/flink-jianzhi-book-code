package com.example._06state.keyed;

import com.example.util.pojo.WaterSensor;
import com.example.util.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @Description 关于状态编程的代码示例:ValueState
 * 不想每次pv+1后就把结果发送到下游，而是使用状态保存的定时器时间戳值，每隔10s输出结果
 * 当定时器触发向下游发送数据后，清空定时器时间戳状态变量
 * @Author kerry
 * @Date 2024/4/23 16:19
 */
public class ValueStateExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (water, l) -> water.time));

        // 1. 统计每个sensor的pv，每隔10s输出结果
        stream.keyBy(data-> data.id)
                .process(new PeriodicPvResult())
                .print();

        env.execute();
    }

    /**
     * 1. 注册定时器，周期性输出pv
     */
    public static class PeriodicPvResult extends KeyedProcessFunction<String, WaterSensor, String>{

        // 需要将状态定义为类的属性，方便其他方法访问；但外部不能直接获取状态，故在外部声明状态对象，在open通过运行时上下文获取状态对象
        ValueState<Long> countState;
        ValueState<Long> timerState;

        // 定义状态失效时间
        StateTtlConfig config = StateTtlConfig.newBuilder(Time.seconds(10))
                // 设置更新类型：什么时候更新失效时间 OnCreateAndWrite-创建、更改状态时-默认值；OnReadAndWrite-无论读写都会更新，即只要访问就表明它是活跃的，从而延迟TTL
                .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                // 设置状态可见性：状态清除非实时导致状态过期后还可能存在 NeverReturnExpired-从不返回过期值，只要过期就认为被清除；ReturnExpiredIfNotCleanedUp-过期状态存在就返回
                .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                .build();

        // open方法只会调用一次，故在open中获取状态对象
        @Override
        public void open(Configuration parameters){
            ValueStateDescriptor<Long> descriptorCount = new ValueStateDescriptor<>("count", Types.LONG);
            ValueStateDescriptor<Long> descriptorTimer = new ValueStateDescriptor<>("timerTs", Types.LONG);
            countState = getRuntimeContext().getState(descriptorCount);
            timerState = getRuntimeContext().getState(descriptorTimer);

            descriptorCount.enableTimeToLive(config);
        }

        @Override
        public void processElement(WaterSensor waterSensor,Context context, Collector<String> collector) throws Exception {
            // 更新count值
            Long count = countState.value();
            if(count == null){
                countState.update(1L);
            }else {
                countState.update(count + 1);
            }

            // 注册定时器
            if(timerState.value() == null){
                context.timerService().registerProcessingTimeTimer(waterSensor.time + 10 * 1000L);
                timerState.update(waterSensor.time + 10 * 1000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext context, Collector<String> collector) throws IOException {
            collector.collect(context.getCurrentKey() + " pv: " + countState.value());
            timerState.clear();
        }
    }
}
