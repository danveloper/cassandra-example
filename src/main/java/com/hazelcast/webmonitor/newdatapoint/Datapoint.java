package com.hazelcast.webmonitor.newdatapoint;

import java.util.HashMap;
import java.util.Map;

public class Datapoint {
    public String metricName;
    public long timestampMs;
    public long value;
    public Map<String, String> tags = new HashMap<String, String>();

    public static void main(String[] args) {
        Datapoint datapoint = new Datapoint();
        datapoint.metricName = "IMap.readCount";
        datapoint.value = 10;
        datapoint.timestampMs = System.currentTimeMillis();
        datapoint.tags.put("cluster", "dev");
        datapoint.tags.put("member", "192.168.1.1:5701");
        datapoint.tags.put("id", "map1");
        datapoint.tags.put("company", "Hazelcast");
    }
}
