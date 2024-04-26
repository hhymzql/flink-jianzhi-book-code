package com.example._04process;

import com.example.util.source.ClickSource;
import com.example.util.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * @Description 使用ProcessAllWindowFunction实现热门xxTopN，示例是热门url，这里用ts字段代替，计算ts频率TopN
 * @Author kerry
 * @Date 2024/4/17 17:06
 */
public class TopNbyProcessAllWindowFunction {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> sensorStream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (sensor, l) -> sensor.time));

        // 只需要ts就可以统计数量，所以转换成String直接开窗统计
        sensorStream.map((MapFunction<WaterSensor, String>) waterSensor -> waterSensor.ts)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .process(new ProcessAllWindowFunction<String, String, TimeWindow>() {
                    @Override
                    public void process(Context context, Iterable<String> elements, Collector<String> collector) throws Exception {
                        HashMap<String, Long> tsCountMap = new HashMap<>();
                        // 遍历窗口数据，将频率保持到hashmap中中
                        for (String ts : elements){
                            if(tsCountMap.containsKey(ts)){
                                long count = tsCountMap.get(ts);
                                tsCountMap.put(ts, count + 1L);
                            }else {
                                tsCountMap.put(ts, 1L);
                            }
                        }
                        // 将频率放到ArrayList中进行排序
                        ArrayList<Tuple2<String, Long>> mapList = new ArrayList<>();
                        for (String key : tsCountMap.keySet()){
                            mapList.add(Tuple2.of(key, tsCountMap.get(key)));
                        }
                        mapList.sort((o1, o2) -> o2.f1.intValue() - o1.f1.intValue());
                        // 取排序后的前两名，构建输出结果
                        StringBuilder result = new StringBuilder();
                        result.append("-----------------------\n");
                        for (int i = 0; i < 2; i++) {
                            Tuple2<String, Long> temp = mapList.get(i);
                            String info = "频率No." + (i + 1) +
                                    " ts:" + temp.f0 +
                                    " 频率：" + temp.f1 +
                                    " 窗口结束时间：" + new Timestamp(context.window().getEnd()) + "\n";
                            result.append(info);
                        }
                        collector.collect(result.toString());
                    }
                }).print();

        env.execute();
    }
}
