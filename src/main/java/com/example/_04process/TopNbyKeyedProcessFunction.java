package com.example._04process;

import com.example._03time_and_window.WindowExample;
import com.example.util.pojo.TsViewCount;
import com.example.util.pojo.WaterSensor;
import com.example.util.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;

/**
 * @Description 基于ProcessAllWindow的TopN优化
 * 1、按键分区，增加并行度
 * 2、避免使用hashmap，增量聚合——AggregateFunction
 *
 * 问题：按键分区后，窗口只会有一个ts的数据，无法直接使用ProcessWindowFunction进行排序
 * 操作：先对ts统计频率；再将统计结果收集后，排序输出最终结果
 * @Author kerry
 * @Date 2024/4/18 11:09
 */
public class TopNbyKeyedProcessFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (sensor, l) -> sensor.time));

        // 对结果中同一个窗口的统计数据进行排序
        // 使用增量函数+全窗口函数进行窗口聚合，得到每个ts在每个统计窗口内的频率，包装成TsViewCount
        SingleOutputStreamOperator<TsViewCount> tsCountStream = sensorStream.keyBy(data -> data.ts)
                .window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new WindowExample.TsViewCountAgg(), new WindowExample.TsViewCountResult());

        // 对窗口进行keyBy操作，对同一窗口的统计结果使用KeyedProcessFunction进行收集并排序输出
        tsCountStream.keyBy(data -> data.windowEnd)
                .process(new TopN(2)).print();

        env.execute();
    }

    /**
     * 自定义处理函数，排序取TopN：
     * KeyedProcessFunction不能保证当前时间段的数据全部到齐，故设置1ms延迟的定时器
     * 在等待过程中用ListState将已到达的数据缓存起来
     */
    public static class TopN extends KeyedProcessFunction<Long, TsViewCount, String>{

        // Top n
        private Integer n;
        // 定义一个列表状态
        private ListState<TsViewCount> tsCountListState;

        public TopN(Integer n) {
            this.n = n;
        }

        /**
         * 将状态列表的定义放到open生命周期方法中
         * @param parameters
         */
        @Override
        public void open(Configuration parameters) {
            // 从环境中获取列表状态句柄
            tsCountListState = getRuntimeContext().getListState(
                    new ListStateDescriptor<TsViewCount>("ts-view-count-list",
                            Types.POJO(TsViewCount.class))
            );
        }

        /**
         * 每到一个TsViewCount就添加到当前的状态列表中
         * 注册一个触发时间为窗口结束时间+1ms的定时器，保证当前窗口所有ts统计结果TsViewCount都到齐了，从列表状态中取出来进行排序
         * @param value
         * @param context
         * @param collector
         * @throws Exception
         */
        @Override
        public void processElement(TsViewCount value, Context context, Collector<String> collector) throws Exception {
            // 将count数据添加到列表状态中，保存起来
            tsCountListState.add(value);
            // 注册windowEnd + 1ms 后的定时器，等待所有数据到齐后开始排序
            // (**定时器会对同一key和时间戳的定时器去重，故设定的windowEnd + 1ms的定时器是一样的最终只会被触发一次，这里的key是windowEnd**)
            collector.collect("windowEnd:" + new Timestamp(context.getCurrentKey()));
            context.timerService().registerProcessingTimeTimer(context.getCurrentKey() + 1);
        }

        /**
         * 从列表状态中取出来进行排序
         * @param timestamp
         * @param ctx
         * @param out
         * @throws Exception
         */
        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<String> out) throws Exception {
            // 将数据从列表状态变量中取出放入list方便排序
            ArrayList<TsViewCount> tsViewCounts = new ArrayList<>();
            for (TsViewCount count: tsCountListState.get()) {
                tsViewCounts.add(count);
            }
            // 清空状态，释放资源
            tsCountListState.clear();
            tsViewCounts.sort((o1,o2) -> o2.count.intValue() - o1.count.intValue());

            // 取排序后的前n名，构建输出结果
            StringBuilder result = new StringBuilder();
            result.append("-----------------------\n");
            result.append("窗口结束时间：").append(new Timestamp(timestamp - 1)).append("\n");
            for (int i = 0; i < this.n; i++) {
                TsViewCount tsViewCount = tsViewCounts.get(i);
                String info = "频率No." + (i + 1) +
                        " ts:" + tsViewCount.ts +
                        " 频率：" + tsViewCount.count + "\n";
                result.append(info);
            }
            out.collect(result.toString());
        }
    }
}
