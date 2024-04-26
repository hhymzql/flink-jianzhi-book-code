package com.example._04process;

import com.example.util.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @Description 最灵活的处理函数示例，使用ProcessFunction可以实现基本算子，如flatMap，filter等，窗口也可以
 * @Author kerry
 * @Date 2024/4/16 14:58
 */
public class ProcessFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

//        env.addSource(new ClickSource())
//                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
//                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (water, l) -> water.time))
//                        .process(new ProcessFunction<WaterSensor, String>() {
//                            /**
//                             * ProcessFunction的内部方法一： 必须实现的内部抽象方法
//                             * 定义了对元素操作的核心逻辑
//                             * @param waterSensor 当前流中的输入元素，即正在处理的数据，其类型与流中数据类型一致
//                             * @param context 内部抽象类Context，表示当前运行的上下文，提供了用于查询时间和注册定时器的定时服务TimeService，以及output
//                             * @param collector 收集器，用于返回数据，使用方法与flatMap算子一样
//                             * @throws Exception
//                             */
//                            @Override
//                            public void processElement(WaterSensor waterSensor, ProcessFunction<WaterSensor, String>.Context context, Collector<String> collector) throws Exception {
//                                if("11".equals(waterSensor.id)){
//                                    // 输出一次
//                                    collector.collect(waterSensor.ts);
//                                }else if("12".equals(waterSensor.id)){
//                                    // 输出两次
//                                    collector.collect(waterSensor.ts);
//                                    collector.collect(waterSensor.ts);
//                                }
//                                System.out.println(context.timerService().currentWatermark());
//                            }
//
//                            /**
//                             * ProcessFunction的内部方法二： 非必须实现的非抽象方法
//                             * 定义了定时触发的操作；只在注册好的定时器触发时才会被调用；基于时间的回调方法，在事件时间下由水位线触发
//                             * 当按时间分组，用ontimer做定时触发时，就实现了窗口函数
//                             * @param timestampe 设定好的触发时间
//                             * @param context
//                             * @param out
//                             */
//                            @Override
//                            public void onTimer(long timestampe, ProcessFunction<WaterSensor, String>.OnTimerContext context, Collector<String> out){
//
//                            }
//                        })
//                .print();

        // 处理时间定时器
        // 处理时间语义，不需要分配时间戳和水位线
//        SingleOutputStreamOperator<WaterSensor> stream = env.addSource(new ClickSource());
//
//        stream.keyBy(data -> true)
//                        .process(new KeyedProcessFunction<Boolean, WaterSensor, Object>() {
//                            @Override
//                            public void processElement(WaterSensor waterSensor, KeyedProcessFunction<Boolean, WaterSensor, Object>.Context context, Collector<Object> collector) throws Exception {
//                                long currTs = context.timerService().currentProcessingTime();
//                                collector.collect("数据到达，到达时间：" + new Timestamp(currTs));
//                                // 注册一个10s后的定时器
//                                context.timerService().registerProcessingTimeTimer(currTs + 10 * 1000L);
//                            }
//                            @Override
//                            public void onTimer(long time, KeyedProcessFunction<Boolean, WaterSensor, Object>.OnTimerContext ctx, Collector<Object> out){
//                                out.collect("定时器触发，触发时间：" + new Timestamp(time));
//                            }
//                        }).print();

        // 处理函数也可以将数据放到侧输出流
        OutputTag<String> outputTag = new OutputTag<String>("side-output");

        // 事件时间语义定时器
        // 基于KeyedStream定义时间定时器
        SingleOutputStreamOperator<String> process = env.addSource(new CustomSource()).assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (sensor, l) -> sensor.time))
                .keyBy(data -> true)
                .process(new KeyedProcessFunction<Boolean, WaterSensor, String>() {

                    @Override
                    public void processElement(WaterSensor waterSensor, Context context, Collector<String> collector) {
                        collector.collect("数据到达，时间戳为：" + context.timestamp());
                        collector.collect("数据到达，水位线为：" + context.timerService().currentWatermark() + "\n----------分割线---------");
                        // 注册一个10s后的定时器
                        context.timerService().registerProcessingTimeTimer(context.timestamp() + 10 * 1000L);
                        context.output(outputTag, "side-output:" + waterSensor.getId());
                    }

                    // 定时器真正派上用场
                    @Override
                    public void onTimer(long time, OnTimerContext ctx, Collector<String> out) {
                        out.collect("定时器触发，触发时间：" + time);
                    }
                });
        SideOutputDataStream<String> sideOutput = process.getSideOutput(outputTag);

        process.print();
        env.execute();

    }

    public static class CustomSource implements SourceFunction<WaterSensor>{

        @Override
        public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
            sourceContext.collect(new WaterSensor("s1", "11", 1, 1000L));
            Thread.sleep(5000L);

            sourceContext.collect(new WaterSensor("s1", "11", 1, 11000L));
            Thread.sleep(5000L);

            sourceContext.collect(new WaterSensor("s2", "12", 1, 11001L));
            Thread.sleep(5000L);
        }

        @Override
        public void cancel() {

        }
    }
}
