package com.example._06state.keyed;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;


/**
 * @Description 关于状态编程的代码示例:ListState
 * 使用ListState实现一个Flink SQL：
 *      select * from a inner join b where a.id = b.id
 *      sql语句的实现过程中，flink会把流a 流b 的所有数据都保存下来，进行连结，需慎用
 * @Author kerry
 * @Date 2024/4/23 17:42
 */
public class ListStateExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple3<String,String,Long>> stream1 = env.fromElements(
                Tuple3.of("a", "stream1", 1000L),
                Tuple3.of("b", "stream1", 2000L))
                        .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String,String,Long>>) (e, l) -> e.f2));

        SingleOutputStreamOperator<Tuple3<String,String,Long>> stream2 = env.fromElements(
                        Tuple3.of("a", "stream2", 3000L),
                        Tuple3.of("b", "stream2", 4000L))
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<String,String,Long>>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<Tuple3<String,String,Long>>) (e, l) -> e.f2));

        // 将ab key相同的数据连接起来
        stream1.keyBy(data-> data.f0)
                .connect(stream2.keyBy(data -> data.f0))
                .process(new SqlResult())
                .print();

        env.execute();
    }

    public static class SqlResult extends CoProcessFunction<Tuple3<String, String, Long>, Tuple3<String, String, Long>, String>{

        // 需要将状态定义为类的属性，方便其他方法访问；但外部不能直接获取状态，故在外部声明状态对象，在open通过运行时上下文获取状态对象
        ListState<Tuple3<String, String, Long>> stream1State;
        ListState<Tuple3<String, String, Long>> stream2State;

        // open方法只会调用一次，故在open中获取状态对象
        @Override
        public void open(Configuration parameters){
            stream1State = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("stream1-list", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );
            stream2State = getRuntimeContext().getListState(
                    new ListStateDescriptor<>("stream2-list", Types.TUPLE(Types.STRING, Types.STRING, Types.LONG))
            );
        }

        @Override
        public void processElement1(Tuple3<String, String, Long> left, Context context, Collector<String> collector) throws Exception {
            stream1State.add(left);
            for (Tuple3<String, String, Long> right: stream2State.get()) {
                collector.collect(left + "=>" + right);
            }
        }

        @Override
        public void processElement2(Tuple3<String, String, Long> right, Context context, Collector<String> collector) throws Exception {
            stream2State.add(right);
            for (Tuple3<String, String, Long> left: stream1State.get()) {
                collector.collect(left + "=>" + right);
            }
        }
    }
}
