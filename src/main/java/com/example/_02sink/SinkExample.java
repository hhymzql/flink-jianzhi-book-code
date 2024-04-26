package com.example._02sink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import com.example.util.pojo.WaterSensor;

/**
 * @Description 输出算子使用示例
 * @Author kerry
 * @Date 2024/4/8 17:08
 */
public class SinkExample {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        DataStream<WaterSensor> stream = env.fromElements(
                new WaterSensor("sensor_1", "11", 111),
                new WaterSensor("sensor_1", "12", 122),
                new WaterSensor("sensor_2", "21", 211),
                new WaterSensor("sensor_2", "22", 222),
                new WaterSensor("sensor_3", "3", 31),
                new WaterSensor("sensor_4", "4", 41)
        );

        // 将DataStream<com.example.util.pojo.WaterSensor>转换为DataStream<String>
        DataStream<String> stringDataStream = stream.map((MapFunction<WaterSensor, String>) Object::toString);

        // 输出到文件
//        FileSink<String> sink = FileSink
//                .forRowFormat(new Path("./outputPath"), new SimpleStringEncoder<String>("UTF-8"))
//                .withRollingPolicy(
//                        DefaultRollingPolicy.builder()
//                                .withRolloverInterval(Duration.ofMinutes(15))
//                                .withInactivityInterval(Duration.ofMinutes(5))
//                                .withMaxPartSize(MemorySize.ofMebiBytes(1024))
//                                .build())
//                .build();
//        stringDataStream.sinkTo(sink);

        // 输出到kafka
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("192.168.50.156:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("test-sink-to-kafka")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();
        stringDataStream.sinkTo(sink);

        // 输出到Redis
//        FlinkJedisPoolConfig config = new FlinkJedisPoolConfig.Builder().setHost("192.168.50.156").build();
//        env.addSource(new ClickSource()).addSink(new RedisSink<>(config, new MyRedisMapper()));

        // 自定义输出
//        env.fromElements("hello","world").addSink(
//                new RichSinkFunction<String>() {
//                    // 管理hbase配置信息，这里因为Configuration重名问题，将类以完整路径引入
//                    public org.apache.hadoop.conf.Configuration configuration;
//                    public Connection connection;
//                    @Override
//                    public void open(Configuration parameters) throws Exception {
//                        super.open(parameters);
//                        configuration = HBaseConfiguration.create();
//                        configuration.set("hbase.zookeeper.quorum", "192.168.50.156:2181");
//                        connection = ConnectionFactory.createConnection(configuration);
//                    }
//
//                    @Override
//                    public void close() throws Exception {
//                        super.close();
//                        connection.close();
//                    }
//
//                    @Override
//                    public void invoke(String value, Context context) throws Exception {
//                        Table table = connection.getTable(TableName.valueOf("test"));
//                        Put put = new Put("rowKey".getBytes(StandardCharsets.UTF_8));//指定rowKey
//                        put.addColumn("info".getBytes(StandardCharsets.UTF_8),//指定列名
//                                value.getBytes(StandardCharsets.UTF_8),
//                                "1".getBytes(StandardCharsets.UTF_8));
//                        table.put(put);
//                        table.close();
//                    }
//                }
//        );

        env.execute();
    }
}
