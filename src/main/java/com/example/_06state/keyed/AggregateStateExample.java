package com.example._06state.keyed;

import com.example.util.pojo.WaterSensor;
import com.example.util.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;

/**
 * @Description 关于状态编程的代码示例:MapState
 * 模拟计数窗口求平均值的实现
 * @Author kerry
 * @Date 2024/4/24 13:29
 */
public class AggregateStateExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (water, l) -> water.time));

        // 统计sensor频次，对每5条数据统计一次平均时间戳
        stream.keyBy(data-> data.id)
                .flatMap(new AvgTsResult())
                .print();

        env.execute();
    }

    /**
     * 1. 注册定时器，周期性输出pv
     */
    public static class AvgTsResult extends RichFlatMapFunction<WaterSensor, String> {

        // 定义一个值状态，用来保存sensor频次
        ValueState<Long> countState;

        // 定义聚合状态，用来计算平均时间戳
        AggregatingState<WaterSensor, Long> avgTsAggState;

        // open方法只会调用一次，故在open中获取状态对象
        @Override
        public void open(Configuration parameters){
            avgTsAggState = getRuntimeContext().getAggregatingState(
                    new AggregatingStateDescriptor<>("avg", new AggregateFunction<WaterSensor, Tuple2<Long, Long>, Long>() {
                        @Override
                        public Tuple2<Long, Long> createAccumulator() {
                            return Tuple2.of(0L, 0L);
                        }

                        @Override
                        public Tuple2<Long, Long> add(WaterSensor waterSensor, Tuple2<Long, Long> accumulator) {
                            return Tuple2.of(accumulator.f0 + waterSensor.time, accumulator.f1 + 1);
                        }

                        // 在count=5时调用
                        @Override
                        public Long getResult(Tuple2<Long, Long> accumulator) {
                            return accumulator.f0 / accumulator.f1;
                        }

                        @Override
                        public Tuple2<Long, Long> merge(Tuple2<Long, Long> longLongTuple2, Tuple2<Long, Long> acc1) {
                            return null;
                        }
                    }, Types.TUPLE(Types.LONG, Types.LONG)));

            countState = getRuntimeContext().getState(new ValueStateDescriptor<Long>("count", Long.class));
        }


        @Override
        public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
            Long count = countState.value();
            if(count == null){
                count = 1L;
            }else {
                count++;
            }

            countState.update(count);
            avgTsAggState.add(waterSensor);

            if(count == 5){
                collector.collect(waterSensor.id + "平均时间戳:" + new Timestamp(avgTsAggState.get()));
                countState.clear();
            }
        }
    }
}
