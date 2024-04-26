package com.example._06state.operator;

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

/**
 * @Description 算子状态的一个特殊状态--广播状态示例
 * 根据特定规则，匹配用户行为
 * @Author kerry
 * @Date 2024/4/26 16:35
 */
public class BroadcastStateExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 用户行为事件流
        DataStreamSource<Action> actionStream = env.fromElements(
                new Action("Ali", "login"),
                new Action("Ali", "pay"),
                new Action("Bob", "login"),
                new Action("Bob", "buy")
        );

        // 行为模式流，检测的标准(先buy，后pay，则只会匹配到一条数据，说明模式顺序和行为顺序需保持一致？？)
        DataStreamSource<Pattern> patternStream = env.fromElements(
                new Pattern("login", "pay"),
                new Pattern("login", "buy")
        );

        // 定义广播状态的描述器，创建广播流（广播状态只保存了一个Pattern，不关心key，故设为Void）
        MapStateDescriptor<Void, Pattern> bcStateDescriptor = new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class));
        BroadcastStream<Pattern> bcPatterns = patternStream.broadcast(bcStateDescriptor);

        // 将事件流和广播流连接起来处理
        actionStream.keyBy(data -> data.userId)
                .connect(bcPatterns)
                .process(new PatternEvaluator())
                .print();

        env.execute();
    }

    public static class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>{

        // 保存之前的一次行为
        ValueState<String> prevActionState;

        @Override
        public void open(Configuration configuration) throws Exception {
            prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAction", Types.STRING));
        }

        /**
         * 正常处理数据的一条流
         * @param action 当前到来的流数据
         * @param context
         * @param collector
         * @throws Exception
         */
        @Override
        public void processElement(Action action, ReadOnlyContext context, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            // getBroadcastState 获取当前的广播状态（只读）
            Pattern pattern = context.getBroadcastState(new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class))).get(null);
            String prevAction = prevActionState.value();
            if(pattern != null && prevAction != null){
                // 如果前后两次行为都符合模式定义，则输出一组匹配
                System.out.println(pattern.action1 + "-1--" + pattern.action2);
                System.out.println(prevAction + "--2-" + action.action);
                if(pattern.action1.equals(prevAction) && pattern.action2.equals(action.action)){
                    collector.collect(new Tuple2<>(context.getCurrentKey(), pattern));
                }
            }
            prevActionState.update(action.action);
        }

        /**
         * 处理广播流，用新规则来更新广播状态
         * @param pattern 广播流中的规则或配置数据
         * @param context
         * @param collector
         * @throws Exception
         */
        @Override
        public void processBroadcastElement(Pattern pattern, Context context, Collector<Tuple2<String, Pattern>> collector) throws Exception {
            // getBroadcastState 获取当前的广播状态
            BroadcastState<Void, Pattern> bcState = context.getBroadcastState(new MapStateDescriptor<>("patterns", Types.VOID, Types.POJO(Pattern.class)));
            // 将广播状态更新成当前的pattern
            bcState.put(null, pattern);
        }
    }

    /**
     *  用户行为事件类
     */
    public static class Action{

        public String userId;
        public String action;

        public Action() {}

        public Action(String userId, String action) {
            this.userId = userId;
            this.action = action;
        }

        @Override
        public String toString() {
            return "Action{" +
                    "userId='" + userId + '\'' +
                    ", action='" + action + '\'' +
                    '}';
        }
    }

    /**
     * 行为模式类，包含先后发生的两个行为
     */
    public static class Pattern{

        public String action1;
        public String action2;

        public Pattern() {}

        public Pattern(String action1, String action2) {
            this.action1 = action1;
            this.action2 = action2;
        }

        @Override
        public String toString() {
            return "Pattern{" +
                    "action1='" + action1 + '\'' +
                    ", action2='" + action2 + '\'' +
                    '}';
        }
    }
}
