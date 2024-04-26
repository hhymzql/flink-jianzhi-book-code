package com.example.util.source;

import com.example.util.pojo.WaterSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;

import java.util.Calendar;
import java.util.Random;

/**
 * @Description 自定义数据源中自带发送水位线
 * @Author kerry
 * @Date 2024/4/11 11:11
 */
public class ClickSourceWithWatermark implements SourceFunction<WaterSensor> {

    private Boolean running = true;
    @Override
    public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
        Random random = new Random();
        String[] ids = {"s1,s2,s3,s4,s5"};
        String[] tss = {"1","2","3","4","5"};
        int[] vss = {11,22,33,44,55};
        while (running){
            long currTs = Calendar.getInstance().getTimeInMillis();
            WaterSensor waterSensor = new WaterSensor(ids[random.nextInt(ids.length)],
                    tss[random.nextInt(tss.length)],
                    vss[random.nextInt(tss.length)], currTs);
            // 使用collectWithTimestamp将数据发送出去并指明数据中的时间戳字段
            sourceContext.collectWithTimestamp(waterSensor, waterSensor.getTime());
            // 发送水位线
            sourceContext.emitWatermark(new Watermark(waterSensor.getTime() - 1L));
            Thread.sleep(1000L);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
