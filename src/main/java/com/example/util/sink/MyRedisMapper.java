package com.example.util.sink;

import com.example.util.pojo.WaterSensor;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
/**
 * @Description redis映射类接口(说明怎样将数据转换成可以写入Redis的类型)
 * @Author kerry
 * @Date 2024/4/9 15:37
 */
public class MyRedisMapper implements RedisMapper<WaterSensor> {

    @Override
    public String getKeyFromData(WaterSensor waterSensor){
        return waterSensor.getId();
    }

    @Override
    public String getValueFromData(WaterSensor waterSensor){
        return waterSensor.getId();
    }

    @Override
    public RedisCommandDescription getCommandDescription(){
        // 保存到redis是调用的命令是HSET，表名为 sensors
        return new RedisCommandDescription(RedisCommand.HSET, "sensors");
    }
}
