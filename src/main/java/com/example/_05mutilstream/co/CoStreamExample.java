package com.example._05mutilstream.co;

import com.example.util.pojo.WaterSensor;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * @Description 实现合流操作：union不能改变数据类型、connect可以改变数据类型
 * @Author kerry
 * @Date 2024/4/22 14:42
 */
public class CoStreamExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 1、使用union实现合流操作，查看不同流中的水位线进展
        SingleOutputStreamOperator<WaterSensor> stream1 = env.socketTextStream("127.0.0.1", 7777)
                .map(data -> {
            String[] input = data.split(",");
            return new WaterSensor(input[0].trim(), input[1].trim(), Integer.parseInt(input[2].trim()), Long.parseLong(input[3].trim()));
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (sensor, l) -> sensor.getTime()));

        SingleOutputStreamOperator<WaterSensor> stream2 = env.socketTextStream("127.0.0.2", 7777)
                .map(data -> {
            String[] input = data.split(",");
            return new WaterSensor(input[0].trim(), input[1].trim(), Integer.parseInt(input[2].trim()), Long.parseLong(input[3].trim()));
        })
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forBoundedOutOfOrderness(Duration.ofSeconds(5))
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (sensor, l) -> sensor.getTime()));

        // 水位线在两条流中水位线最小值增大时才会向前推进
        stream1.union(stream2).process(new ProcessFunction<WaterSensor, Object>() {
            @Override
            public void processElement(WaterSensor waterSensor, Context context, Collector<Object> collector) throws Exception {
                collector.collect("水位线：" + context.timerService().currentWatermark());
            }
        }).print();

        // 2、使用connect实现合流操作，stream1.connect(stream2) = ConnectedStreams，只能连接2个
        // 两个stream内部数据类型不变，如果想得到新的dataStream，还需定义co-process操作，得到统一操作
        // 示例：将Integer流和Long流，转换成String输出，
        DataStream<Integer> stream3 = env.fromElements(1,2,3);
        DataStream<Long> stream4 = env.fromElements(1L,2L,3L);
        ConnectedStreams<Integer, Long> connectStreams = stream3.connect(stream4);
        connectStreams.map(new CoMapFunction<Integer, Long, String>(){

            @Override
            public String map1(Integer value) throws Exception {
                return "Integer:" + value;
            }

            @Override
            public String map2(Long value) throws Exception {
                return "Long" + value;
            }
        }).print();

        // 使用keyby将两条流中key相同的数据放一起，对来源的流再各自做处理；与keyby之后再connect效果是一样的

        env.execute();
    }
}
