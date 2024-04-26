package com.example._03time_and_window;

import com.example.util.source.ClickSource;
import com.example.util.pojo.TsViewCount;
import com.example.util.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.HashSet;

/**
 * @Description 窗口使用示例
 * @Author kerry
 * @Date 2024/4/11 16:05
 */
public class WindowExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 从自定义数据源读取数据，提取时间戳，周期性生成水位线（自定义数据源是有序的，故这里设置延迟时间为0）
        SingleOutputStreamOperator<WaterSensor> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTime()));

        // 窗口函数:增量聚合函数(更高效)：对每条数据增量处理，保留中间状态、全窗口函数：窗口全部数据收集齐后，再统一处理

        // 增量聚合函数--1、归约函数reduce（类似wordCount）——最终输出结果类型与输入类型必须保持一致，中间聚合状态数据类型也是一样的
        stream.map(new MapFunction<WaterSensor, Tuple2<String, Long>>() {
                    // 此处map不可使用lambda表达式
                    @Override
                    public Tuple2<String, Long> map(WaterSensor waterSensor) throws Exception {
                        return Tuple2.of(waterSensor.getId(), 1L);
                    }
                })
                .keyBy(r -> r.f0)
                // 滚动事件时间窗口
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                // 窗口函数：定义累加规则，当窗口闭合时，向下游发送累计结果
                .reduce((ReduceFunction<Tuple2<String, Long>>) (t1, t2) -> Tuple2.of(t1.f0, t1.f1 + t2.f1))
                .print();

        // 增量聚合函数--2、聚合函数aggregate ——没有数据类型限制，更加灵活
        // 为所有数据设置相同的key，发送到同一个分区统计PV、UV后相除
        stream.keyBy(data -> true)
                        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(2)))
                                // 此处还可以传入第二个参数：全窗口函数WindowFunction or ProcessWindowFunction
                                .aggregate(new AvgPv())
                                        .print();

        // 全窗口函数——processWindowFunction
        // 为所有数据设置相同的key，按窗口统计UV
        stream.keyBy(data -> true)
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .process(new UvCountByWindow())
                                .print();

        // 增量聚合窗口 + 全窗口——按照ts分组，统计10s内ts出现的频率，每5s更新一次
        stream.keyBy(data -> data.ts)
                        .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                                .aggregate(new TsViewCountAgg(), new TsViewCountResult())
                                        .print();

        // 窗口较大时会延迟看到计算结果，可以自定义触发器，每1秒触发一次窗口计算查看结果
        stream.keyBy(data -> data.ts).window(TumblingEventTimeWindows.of(Time.seconds(10)))
                        .trigger(new MyTrigger())
                                .process(new WindowResult())
                                        .print();

        // 将迟到数据放入侧输出流
        OutputTag outputTag = new OutputTag<WaterSensor>("late"){};
        SingleOutputStreamOperator aggregate = stream.keyBy(data -> data.id)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(outputTag)
                .aggregate(new AvgPv());
        SideOutputDataStream sideOutput = aggregate.getSideOutput(outputTag);

        env.execute();
    }

    /**
     * AggregateFunction 工作原理：
     * 1、首先调用createAccumulator为任务初始化一个状态（累加器）；
     * 2、后每来一个数据调用一次add，对数据进行聚合，更新状态中的结果；
     * 3、最后等窗口需要输出时，调用getResult得到结果
     * 同ReduceFunction相同，也是增量式聚合，但输入、中间状态、输出数据呢类型不加限制，使用更加灵活
     */
    public static class AvgPv implements AggregateFunction<WaterSensor, Tuple2<HashSet<String>, Long>, Double>{

        /**
         * 创建一个累加器，即为聚合创建一个初始状态，每次聚合任务只会调用一次
         * @return
         */
        @Override
        public Tuple2<HashSet<String>, Long> createAccumulator() {
            // 创建累加器
            return Tuple2.of(new HashSet<String>(), 0L);
        }

        /**
         * 将后续的数据添加到累计器中，基于初始状态对新数据进一步聚合；每条数据到来都会调用
         * @param waterSensor 新到的数据
         * @param accumulator 当前累加器
         * @return 新的累加器值
         */
        @Override
        public Tuple2<HashSet<String>, Long> add(WaterSensor waterSensor, Tuple2<HashSet<String>, Long> accumulator) {
            // 对属于本窗口的数据，来一条累加一次，并返回累加器
            accumulator.f0.add(waterSensor.getId());
            return Tuple2.of(accumulator.f0, accumulator.f1 + 1L);
        }

        /**
         * 从累加器中提取聚合的输出结果；只在窗口要输出结果时调用
         * @param accumulator
         * @return
         */
        @Override
        public Double getResult(Tuple2<HashSet<String>, Long> accumulator) {
            // 当窗口闭合时，增量聚合结束，将计算结果发送到下游
            return (double) accumulator.f1 / accumulator.f0.size();
        }

        /**
         * 合并两个累加器；合并窗口时调用，最常见的合并窗口场景是会话窗口
         * @param hashSetLongTuple2
         * @param acc1
         * @return 合并后的状态的累加器
         */
        @Override
        public Tuple2<HashSet<String>, Long> merge(Tuple2<HashSet<String>, Long> hashSetLongTuple2, Tuple2<HashSet<String>, Long> acc1) {
            // 没有涉及到会话窗口，merge方法可以不做任何操作
            return null;
        }
    }

    /**
     * ProcessWindowFunction
     * 1、可以拿到窗口全部数据
     * 2、包含强大的上下文对象
     * 运行效率低，很少直接单独使用，往往和增量聚合函数结合使用
     */
    public static class UvCountByWindow extends ProcessWindowFunction<WaterSensor, String, Boolean, TimeWindow> {

        @Override
        public void process(Boolean aBoolean, ProcessWindowFunction<WaterSensor, String, Boolean, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<String> collector) throws Exception {

            // 去重
            HashSet<String> idSet = new HashSet<>();
            for(WaterSensor sensor: iterable){
                idSet.add(sensor.getId());
            }

            // 结合窗口信息包装输出内容
            long start = context.window().getStart();
            long end = context.window().getEnd();
            collector.collect("窗口：" + new Timestamp(start) + "~" + new Timestamp(end) + "的独立访客数量是：" + idSet.size());
        }
    }

    /**
     * AggregateFunction + ProcessWindowFunction
     * 使用AggregateFunction聚合
     */
    public static class TsViewCountAgg implements AggregateFunction<WaterSensor, Long, Long>{

        @Override
        public Long createAccumulator() {
            return 0L;
        }

        @Override
        public Long add(WaterSensor waterSensor, Long accumulator) {
            return accumulator + 1;
        }

        @Override
        public Long getResult(Long accumulator) {
            return accumulator;
        }

        @Override
        public Long merge(Long aLong, Long acc1) {
            return null;
        }
    }

    /**
     * AggregateFunction + ProcessWindowFunction
     * 使用ProcessWindowFunction结合窗口信息包装成TsViewCount输出
     */
    public static class TsViewCountResult extends ProcessWindowFunction<Long, TsViewCount, String, TimeWindow>{

        @Override
        public void process(String s, ProcessWindowFunction<Long, TsViewCount, String, TimeWindow>.Context context, Iterable<Long> elements, Collector<TsViewCount> collector) throws Exception {
            Long start = context.window().getStart();
            Long end = context.window().getEnd();
            // 迭代器中资源一个元素，就是增量聚合函数的计算结果
            collector.collect(new TsViewCount(s, elements.iterator().next(), start, end));
        }
    }

    /**
     *
     */
    public static class WindowResult extends ProcessWindowFunction<WaterSensor, TsViewCount, String, TimeWindow> {

        @Override
        public void process(String s, ProcessWindowFunction<WaterSensor, TsViewCount, String, TimeWindow>.Context context, Iterable<WaterSensor> iterable, Collector<TsViewCount> collector) throws Exception {
            collector.collect(
                    new TsViewCount(
                            s,
                            // 获取迭代器中元素个数
                            iterable.spliterator().getExactSizeIfKnown(),
                            context.window().getStart(),
                            context.window().getEnd()
                    )
            );
        }
    }

    /**
     * 自定义触发器 示例
     * Trigger-触发器-抽象类--控制窗口什么时候触发 & 定义窗口什么时候关闭    一般不需要自定义
     * TriggerResult enum  {CONTINUE、FIRE-触发、PURGE-清除、FIRE_AND_PURGE}
     */
    public static class MyTrigger extends Trigger<WaterSensor, TimeWindow>{

        /**
         * onElement(TriggerContext)：窗口每来到一个元素，都会调用
         */
        @Override
        public TriggerResult onElement(WaterSensor waterSensor, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-value", Types.BOOLEAN)
            );
            if(isFirstEvent.value() == null){
                for (long i = timeWindow.getStart(); i < timeWindow.getEnd(); i = i + 1000L) {
                    triggerContext.registerEventTimeTimer(i);
                }
                isFirstEvent.update(true);
            }
            return TriggerResult.CONTINUE;
        }

        /**
         * onProcessingTime(TriggerContext)：当注册的处理时间定时器触发时调用
         */
        @Override
        public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) {
            return TriggerResult.CONTINUE;
        }

        /**
         * onEventTime(TriggerContext)：当注册的事件时间定时器触发时调用
         */
        @Override
        public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) {
            return TriggerResult.FIRE;
        }

        /**
         * clear(TriggerContext)：当窗口关闭并销毁时调用
         */
        @Override
        public void clear(TimeWindow timeWindow, TriggerContext triggerContext) {
            ValueState<Boolean> isFirstEvent = triggerContext.getPartitionedState(
                    new ValueStateDescriptor<Boolean>("first-value", Types.BOOLEAN)
            );
            isFirstEvent.clear();
        }
    }
}
