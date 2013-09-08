package com;

import java.util.HashMap;
import java.util.Map;


public class Measurement {
    public long timeMs;
    public String environment;
    public String customer;
    public String machine;
    public String subject;
    public Map<String, Long> map = new HashMap<String,Long>();
}
