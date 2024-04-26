package com.example._01operator;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import com.example.util.pojo.WaterSensor;

/**
 * 算子使用示例
 * @author kerry
 */
public class OperatorExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
//        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        DataStreamSource<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", "11", 11),
                new WaterSensor("sensor_1", "22", 22),
                new WaterSensor("sensor_2", "233", 233),
                new WaterSensor("sensor_2", "8", 23),
                new WaterSensor("sensor_3", "3", 3444),
                new WaterSensor("sensor_4", "4", 444)
        );
//        stream.filter(e-> Objects.equals(e.getId(), "")).print();
//        stream.flatMap(new MyFlatMap()).print();

        DataStreamSource<Tuple2<String, Integer>> stream2 = env.fromElements(
                Tuple2.of("a", 11),
                Tuple2.of("a", 22),
                Tuple2.of("b", 233),
                Tuple2.of("b", 33)
        );
//        stream2.keyBy(r->r.f0).sum(1).print();
//        stream2.keyBy(r->r.f0).sum("f1").print();
//        stream2.keyBy(r->r.f0).min("f1").print();
//        stream2.keyBy(r->r.f0).minBy(1).print();

//        stream.keyBy(com.example.util.pojo.WaterSensor::getId)
//                .reduce((ReduceFunction<com.example.util.pojo.WaterSensor>) (value1, value2) -> {
//                    System.out.println("Demo7_Reduce.reduce1:"+value1.getId());
//                    System.out.println("Demo7_Reduce.reduce2:"+value2.getId());
//                    int maxVc = Math.max(value1.getVc(), value2.getVc());
//                    //实现max(vc)的效果  取最大值，其他字段以当前组的第一个为主
//                    //value1.setVc(maxVc);
//                    // 实现maxBy(vc)的效果  取当前最大值的所有字段
//                    if (value1.getVc() > value2.getVc()){
//                        value1.setVc(maxVc);
//                        System.out.println("11:"+value1.getVc());
//                        System.out.println("12:"+value2.getVc());
//                        return value1;
//                    }else {
//                        System.out.println("21:"+value1.getVc());
//                        System.out.println("22:"+value2.getVc());
//                        value2.setVc(maxVc);
//                        return value2;
//                    }
//                })
//                .print();

        //富函数 拥有生命周期函数，可做更复杂操作
//        stream.map(new RichMapFunction<com.example.util.pojo.WaterSensor, String>() {
//
//            @Override
//            public void open(Configuration parameters) throws Exception{
//                super.open(parameters);
//                System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() + "的任务开始");
//            }
//
//            @Override
//            public String map(com.example.util.pojo.WaterSensor value) {
//                return value.getId();
//            }
//
//            @Override
//            public void close() throws Exception{
//                super.close();
//                System.out.println("索引为" + getRuntimeContext().getIndexOfThisSubtask() + "的任务结束");
//            }
//        }).print();

        // 物理分区算子——随机分区-均匀分布
//        stream2.print().setParallelism(2);
//        stream2.shuffle().print("shuffle").setParallelism(2);
        // 物理分区算子——轮询分区-平均分配（所有范围）
//        stream2.rebalance().print("rebalance").setParallelism(4);
        // 物理分区算子——重缩放分区-平均分配（部分分区内）
//        stream2.rescale().print("rescale").setParallelism(4);
        // 物理分区算子——广播分区-数据在每个分区都有一份
//        stream2.broadcast().print("broadcast").setParallelism(4);
        // 物理分区算子——全局分区-所有数据发送到下游算子的第一个并行子任务中，相当于强行使下游并行度=1
//        stream2.global().print("global").setParallelism(4);
        // 物理分区算子——自定义分区
        env.fromElements(1,2,3,45,5,56,6,7).partitionCustom((Partitioner<Integer>)
                (value, numPartitions) -> value % 2,
                (KeySelector<Integer, Integer>) integer -> integer)
                .print("partitionCustom").setParallelism(2);
        env.execute();
    }

    /**
     * TODO flatmap： 一进多出（包含0出）
     *      对于s1的数据，一进一出
     *      对于s2的数据，一进2出
     *      对于s3的数据，一进0出（类似于过滤的效果）
     * map控制一进一出        =》 使用 return
     * flatmap控制的一进多出  =》 通过 Collector来输出， 调用几次就输出几条
     */

    public static class MyFlatMap implements FlatMapFunction<WaterSensor, String> {

        @Override
        public void flatMap(WaterSensor value, Collector<String> out) throws Exception {

            if (value.getId().equals("sensor_1")) {
                out.collect(String.valueOf(value.getVc()));
            } else if (value.getId().equals("sensor_2")) {
                out.collect(String.valueOf(value.getTs()));
                out.collect(String.valueOf(value.getVc()));
            }

        }
    }
}
