package com.example._03time_and_window;

import com.example.util.source.ClickSource;
import com.example.util.source.ClickSourceWithWatermark;
import com.example.util.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Description 水位线使用示例
 * @Author kerry
 * @Date 2024/4/11 9:26
 */
public class WatermarkExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", "11", 111, 1712801412631L),
                new WaterSensor("sensor_1", "12", 122, 1712801412632L),
                new WaterSensor("sensor_2", "21", 211, 1712801412633L),
                new WaterSensor("sensor_2", "22", 222, 1712801412641L),
                new WaterSensor("sensor_3", "3", 31, 1712801412635L),
                new WaterSensor("sensor_4", "4", 41, 1712801412636L)
        );

        // WatermarkStrategy 包含 TimestampAssigner（提取并分配时间戳） + WatermarkGenerator（生成水位线—onEvent-每条数据到达都会调用，断点式，如设置value=“kerry”时生成一次水位线、onPeriodEmit-周期性生成，周期时间为处理时间）

        // 1、flink内置水位线生成器--周期性生成水位线
        // forBoundedOutOfOrderness(0)等效于forMonotonousTimestamps()
        // 有序流
        stream.assignTimestampsAndWatermarks(
                // 时间戳单调增长，直接使用当前最大时间的时间戳作为水位线--forMonotonousTimestamps
                WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTime())
        ).print();
        // 乱序流
        env.addSource(new ClickSource()).assignTimestampsAndWatermarks(
                // 需要等待数据都到齐，故需设置定量的延迟时间--forBoundedOutOfOrderness
                WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTime())
        ).print();

        // 2、自定义水位线策略
        // 周期性水位线生成器--通过onEvent观察输入事件，在onPeriodicEmit发出水位线
        // 断点式水位线生成器--不停检测onEvent事件，当有事件符合触发条件时发出水位线，不通过onPeriodicEmit发出水位线
        env.addSource(new ClickSource()).assignTimestampsAndWatermarks(new CustomWatermarkStrategy()).print();

        // 3、在自定义数据源中也可以发送水位线
        env.addSource(new ClickSourceWithWatermark()).print();

        env.execute();
    }

    /**
     *  自定义水位线策略
     */
    public static class CustomWatermarkStrategy implements WatermarkStrategy<WaterSensor>{

        @Override
        public WatermarkGenerator<WaterSensor> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            // 断点式生成
            return new CustomPunctuatedGenerator();
            // 周期性生成
//            return new CustomPeriodicGenerator();
        }

        @Override
        public TimestampAssigner<WaterSensor> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return (SerializableTimestampAssigner<WaterSensor>) (waterSensor, l) -> waterSensor.getTime();
        }
    }

    /**
     *  自定义水位线策略--周期性--通过onEvent观察输入事件，在onPeriodicEmit发出水位线
     */
    public static class CustomPeriodicGenerator implements WatermarkGenerator<WaterSensor>{

        // 最大延迟时间
        private Long dalayTime = 5000L;
        // 观察到的最大时间戳
        private Long maxTs = Long.MIN_VALUE + dalayTime + 1L;

        @Override
        public void onEvent(WaterSensor waterSensor, long l, WatermarkOutput watermarkOutput) {
            // 每来一条数据就调用一次
            maxTs = Math.max(waterSensor.getTime(), maxTs);
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // 发出水位线，默认200ms调用一次(通过env.getConfig().setAutoWatermarkInterval(60 * 1000L);设置周期时间，默认200ms)
            watermarkOutput.emitWatermark(new Watermark(maxTs - dalayTime - 1L));
        }
    }

    /**
     *  自定义水位线策略--断点式--通过onEvent观察输入事件，有符合条件事件时在onEvent发出水位线
     */
    public static class CustomPunctuatedGenerator implements WatermarkGenerator<WaterSensor>{
        @Override
        public void onEvent(WaterSensor waterSensor, long l, WatermarkOutput watermarkOutput) {
            // 只有遇到特殊事件才发出水位线
            if("xxx".equals(waterSensor.toString())){
             watermarkOutput.emitWatermark(new Watermark(waterSensor.getTime()- 1));
            }
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput watermarkOutput) {
            // do nothing;
        }
    }
}
