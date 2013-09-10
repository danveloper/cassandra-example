package com.hazelcast.webmonitor;

import java.util.HashMap;
import java.util.Map;


public class Measurements {
    public long timestampMs;

    public String environment;
    public String customer;
    public String machine;
    public String subject;

    public Map<String, Long> map = new HashMap<String,Long>();
}
