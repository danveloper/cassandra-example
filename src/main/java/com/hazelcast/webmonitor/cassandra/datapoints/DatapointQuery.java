package com.hazelcast.webmonitor.cassandra.datapoints;

public class DatapointQuery {
    public String company;
    public String cluster;
    public String member;
    public String id;
    public String metric;
    public long beginMs;
    public long endMs;
    public int maxResult = Integer.MAX_VALUE;

    public DatapointQuery(){}

    public DatapointQuery(DatapointQuery that){
        this(that.company,that.cluster,that.member,that.id,that.metric,that.beginMs,that.endMs,that.maxResult);
    }

    public DatapointQuery(String company, String cluster, String member, String id, String metric, long beginMs, long endMs, int maxResult) {
        this.company = company;
        this.cluster = cluster;
        this.member = member;
        this.id = id;
        this.metric = metric;
        this.beginMs = beginMs;
        this.endMs = endMs;
        this.maxResult = maxResult;
    }

    @Override
    public String toString() {
        return "DatapointQuery{" +
                "company='" + company + '\'' +
                ", cluster='" + cluster + '\'' +
                ", member='" + member + '\'' +
                ", id='" + id + '\'' +
                ", metric='" + metric + '\'' +
                ", beginMs=" + beginMs +
                ", endMs=" + endMs +
                ", maxResult=" + maxResult +
                '}';
    }
}
