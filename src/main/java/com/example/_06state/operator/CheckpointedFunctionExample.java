package com.example._06state.operator;

import com.example.util.pojo.WaterSensor;
import com.example.util.source.ClickSource;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.util.ArrayList;
import java.util.List;

/**
 * @Description 算子状态下的故障恢复
 * 自定义的SinkFunction会在 CheckpointedFunction 中进行数据缓存，后统一下发到下游，示例演示了列表状态的平均分割分组
 * @Author kerry
 * @Date 2024/4/26 15:30
 */
public class CheckpointedFunctionExample {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<WaterSensor>forMonotonousTimestamps()
                        .withTimestampAssigner((SerializableTimestampAssigner<WaterSensor>) (water, l) -> water.time));
        stream.print("input");

        stream.addSink(new BufferingSink(10));

        env.execute();
    }

    public static class  BufferingSink implements SinkFunction<WaterSensor>, CheckpointedFunction{

        private final int threshold;
        private transient ListState<WaterSensor> checkpointedState;
        private final List<WaterSensor> bufferedElements;

        public BufferingSink(int threshold) {
            this.threshold = threshold;
            this.bufferedElements = new ArrayList<>();
        }

        /**
         * 当保存状态快照到检查点时调用，将状态进行外部持久化
         * @param context 可以提供检查点信息，但不能获取状态句柄
         * @throws Exception
         */
        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            checkpointedState.clear();
            // 把当前局部变量中的所有元素写入检查点
            for (WaterSensor sensor: bufferedElements) {
                checkpointedState.add(sensor);
            }
        }

        /**
         * 初始化状态时调用，故障后重启恢复状态时也会调用
         * @param context 函数类进行初始化时的上下文，真正的运行时上下文，提供算子状态存储OperatorStateStore 和 按键分区状态存储KeyedStateStore
         * @throws Exception
         */
        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            // 算子状态的注册方式与按键分区状态类似
            ListStateDescriptor<WaterSensor> descriptor = new ListStateDescriptor<>("buffered-elements", Types.POJO(WaterSensor.class));
            checkpointedState = context.getOperatorStateStore().getListState(descriptor);

            // 判断是从故障恢复还是第一次启动，如果时从故障中恢复，就将ListState中的所有元素添加到局部变量bufferedElements中
            if(context.isRestored()){
                for (WaterSensor sensor: checkpointedState.get()) {
                    bufferedElements.add(sensor);
                }
            }
        }

        @Override
        public void invoke(WaterSensor value, Context context) {
            bufferedElements.add(value);
            if(bufferedElements.size() == threshold){
                for (WaterSensor sensor: bufferedElements) {
                    // 输出到外部系统，这里用控制台打印模拟
                    System.out.println(sensor);
                }
                System.out.println("============输出完毕============");
            }
            bufferedElements.clear();
        }
    }
}
