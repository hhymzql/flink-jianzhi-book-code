package com.example.util.pojo;

import java.sql.Timestamp;

/**
 * @author kerry
 */
public class WaterSensor {

    public String id;
    public int vc;
    public String ts;
    public long time;

    public WaterSensor(){};

    public WaterSensor(String id, String ts, int vc) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
    }
    public WaterSensor(String id, String ts, int vc, long time) {
        this.id = id;
        this.ts = ts;
        this.vc = vc;
        this.time = time;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getVc() {
        return vc;
    }

    public void setVc(int vc) {
        this.vc = vc;
    }

    public String getTs() {
        return ts;
    }

    public void setTs(String ts) {
        this.ts = ts;
    }

    public long getTime() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "com.example.util.pojo.WaterSensor{" +
                "id='" + id + '\'' +
                ", vc=" + vc +
                ", ts=" + ts +
                ", time=" + new Timestamp(time) +
                '}';
    }
}
