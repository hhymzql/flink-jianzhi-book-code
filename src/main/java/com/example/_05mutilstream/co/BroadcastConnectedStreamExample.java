package com.example._05mutilstream.co;

import org.apache.commons.codec.language.bm.Rule;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description 两流连接特殊用法：connect参数是一个广播流
 *                            这种连接方式往往用在需要动态定义某些规则或配置的场景，需要将规则配置广播给所有并行子任务
 *                            下游子任务收到广播规则后会保存成广播状态
 * @Author kerry
 * @Date 2024/4/22 16:41
 */
public class BroadcastConnectedStreamExample {

    public static void main(String[] args) {

        // 详细用法见src/main/java/com/example/_06state/operator/BroadcastStateExample.java
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<Integer> stream = env.fromElements(11, 21, 121);
        DataStreamSource<Integer> ruleStream = env.fromElements(1, 2, 12);

        // 规则数据的广播流 broadcastStream
        MapStateDescriptor<String, Rule> ruleMapStateDescriptor = new MapStateDescriptor<>("rule", Types.STRING, TypeInformation.of(Rule.class));
        BroadcastStream<Integer> broadcastStream = ruleStream.broadcast(ruleMapStateDescriptor);

        stream.connect(broadcastStream).process(new BroadcastProcessFunction<Integer, Integer, String>(){

            /**
             * 正常处理数据的一条流
             * @param integer
             * @param readOnlyContext
             * @param collector
             * @throws Exception
             */
            @Override
            public void processElement(Integer integer, ReadOnlyContext readOnlyContext, Collector<String> collector) throws Exception {

            }

            /**
             * 用新规则来更新广播状态
             * @param integer
             * @param context
             * @param collector
             * @throws Exception
             */
            @Override
            public void processBroadcastElement(Integer integer, Context context, Collector<String> collector) throws Exception {

            }
        });
    }
}
