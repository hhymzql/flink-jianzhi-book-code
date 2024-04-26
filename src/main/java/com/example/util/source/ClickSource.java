package com.example.util.source;

import com.example.util.pojo.WaterSensor;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Calendar;
import java.util.Random;

/**
 * @Description 自定义源算子
 * @Author kerry
 * @Date 2024/4/9 15:50
 */
public class ClickSource implements SourceFunction<WaterSensor> {

    private Boolean running = true;

    @Override
    public void run(SourceContext<WaterSensor> sourceContext) throws Exception {
        Random random = new Random();
        String[] ids = {"s1","s2","s3","s4","s5"};
        String[] tss = {"1","2","3","4","5"};
        int[] vss = {11,22,33,44,55};
        while (running){
            sourceContext.collect(
                    new WaterSensor(ids[random.nextInt(ids.length)],
                    tss[random.nextInt(tss.length)],
                    vss[random.nextInt(vss.length)],
                    Calendar.getInstance().getTimeInMillis()
            ));
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }
}
