package com.example._05mutilstream.co;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

import java.io.IOException;

/**
 * @Description 连接流示例：
 *                 实时对账：app支付操作和第三方支付操作的双流join，两个操作会相互等待5s，如果等不来则输出报警
 * @Author kerry
 * @Date 2024/4/19 17:23
 */
public class CoProcessFunctionExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 模拟来自app的支付日志
        SingleOutputStreamOperator<Tuple3<String, String, Long>> appStream = env.fromElements(
                Tuple3.of("order1", "app", 1000L),
                Tuple3.of("order2", "app", 2000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String, String, Long>>) (s, l) -> s.f2));

        // 模拟来自第三方支付平台的支付日志
        SingleOutputStreamOperator<Tuple4<String, String, String, Long>> thirdPartyStream = env.fromElements(
                Tuple4.of("order1", "third", "success", 3000L),
                Tuple4.of("order3", "third", "success", 4000L)
        ).assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple4<String, String, String, Long>>forMonotonousTimestamps()
                .withTimestampAssigner((SerializableTimestampAssigner<Tuple4<String, String, String, Long>>) (s, l) -> s.f3));

        // 检测同一支付单在两条流中是否匹配，如果不匹配报警
        appStream.connect(thirdPartyStream)
                .keyBy(data -> data.f0, data -> data.f0)
                .process(new OrderMatchResult())
                .print();

        env.execute();
    }

    public static class OrderMatchResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple4<String, String, String, Long>, String> {

        private ValueState<Tuple3<String, String, Long>> appEventState;
        private ValueState<Tuple4<String, String, String, Long>> thirdPartyEventState;

        @Override
        public void open(Configuration parameters){
            appEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple3<String, String, Long>>("app-event", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );
            thirdPartyEventState = getRuntimeContext().getState(
                    new ValueStateDescriptor<Tuple4<String, String, String, Long>>("thirdParty-event", Types.TUPLE(Types.STRING, Types.STRING, Types.STRING, Types.LONG))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> value, Context context, Collector<String> collector) throws Exception {
            // 看另一条流中的事件是否来过
            if(thirdPartyEventState.value() != null){
                collector.collect("对账成功：" + value + " " + thirdPartyEventState.value());
                thirdPartyEventState.clear();
            }else {
                appEventState.update(value);
                // 注册一个5s后的定时器，开始等待另一条流的事件
                context.timerService().registerProcessingTimeTimer(value.f2 + 5000L);
            }
        }

        @Override
        public void processElement2(Tuple4<String, String, String, Long> value, Context context, Collector<String> collector) throws Exception {
            if(appEventState.value() != null){
                collector.collect("对账成功：" + appEventState.value() + " " + value);
                appEventState.clear();
            }else {
                thirdPartyEventState.update(value);
                context.timerService().registerProcessingTimeTimer(value.f3 + 5000L);
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws IOException {
            // 等待5s后，先到的支付状态还在，说明对应的另一方支付消息没来，输出报警信息
            if(appEventState.value() != null){
                out.collect("对账失败：" + appEventState.value() + " 第三方支付平台信息未到达。");
            }
            if(thirdPartyEventState.value() != null){
                out.collect("对账失败：" + thirdPartyEventState.value() + " app信息未到达。");
            }
            appEventState.clear();
            thirdPartyEventState.clear();
        }
    }
}
