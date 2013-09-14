package com.hazelcast.webmonitor.cassandra;

public class Measurement {

    public String metricName;
    public long timestampMs;

    public String cluster;
    public String member;
    public String id;
    public String company;

    public double value;

    public Measurement(){
    }

    public Measurement(Measurement m){
        this(m.metricName,m.timestampMs,m.cluster,m.member,m.id,m.company,m.value);
    }

    public Measurement(String metricName, long timestampMs, String cluster, String member, String id, String company, double value) {
        this.metricName = metricName;
        this.timestampMs = timestampMs;
        this.cluster = cluster;
        this.member = member;
        this.id = id;
        this.company = company;
        this.value = value;
    }
}
