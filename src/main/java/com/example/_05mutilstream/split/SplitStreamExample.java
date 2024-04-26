package com.example._05mutilstream.split;

import com.example.util.pojo.WaterSensor;
import com.example.util.source.ClickSource;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.util.Objects;

/**
 * @Description 使用filter、侧输出流实现分流操作
 * @Author kerry
 * @Date 2024/4/22 14:04
 */
public class SplitStreamExample {

    private static OutputTag<Tuple3<String,String,Long>> s1Tag = new OutputTag<Tuple3<String,String,Long>>("s1-pv"){};
    private static OutputTag<Tuple3<String,String,Long>> s2Tag = new OutputTag<Tuple3<String,String,Long>>("s2-pv"){};

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStreamSource<WaterSensor> sensorStream = env.addSource(new ClickSource());

        // 简单的分流-filter
        DataStream<WaterSensor> s1Stream = sensorStream.filter(e-> Objects.equals(e.id, "s1"));
        s1Stream.print("s1 pv");

        // 使用测输出流的分流-outputTag,输出类型不受限制
        SingleOutputStreamOperator<WaterSensor> processStream = sensorStream.process(
                new ProcessFunction<WaterSensor,WaterSensor>() {
                     @Override
                     public void processElement(WaterSensor value, Context context, Collector<WaterSensor> collector) throws Exception {
                         if(value.id.equals("s1")){
                             context.output(s1Tag, new Tuple3<>(value.id, value.ts, value.time));
                         } else if(value.id.equals("s2")){
                             context.output(s2Tag, new Tuple3<>(value.id, value.ts, value.time));
                         } else {
                             collector.collect(value);
                         }
                     }
                });

        processStream.getSideOutput(s1Tag).print("s1Tag");
        processStream.getSideOutput(s2Tag).print("s2Tag");
        processStream.print("else");

        env.execute();
    }
}
