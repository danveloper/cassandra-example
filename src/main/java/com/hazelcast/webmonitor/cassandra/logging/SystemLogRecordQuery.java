package com.hazelcast.webmonitor.cassandra.logging;

public final class SystemLogRecordQuery {
    private final String company;
    private final String cluster;
    private final String member;
    private final long startMs;
    private final long endMs;
    private final int maxResults;

    public SystemLogRecordQuery(long endMs, long startMs, String member, String cluster, String company, int maxResults) {
        this.endMs = endMs;
        this.startMs = startMs;
        this.member = member;
        this.cluster = cluster;
        this.company = company;
        this.maxResults = maxResults;
    }

    public String getCompany() {
        return company;
    }

    public String getCluster() {
        return cluster;
    }

    public String getMember() {
        return member;
    }

    public long getStartMs() {
        return startMs;
    }

    public long getEndMs() {
        return endMs;
    }

    public int getMaxResults() {
        return maxResults;
    }

    @Override
    public String toString() {
        return "SystemLogRecordQuery{" +
                "company='" + company + '\'' +
                ", cluster='" + cluster + '\'' +
                ", member='" + member + '\'' +
                ", startMs=" + startMs +
                ", endMs=" + endMs +
                ", maxResults=" + maxResults +
                '}';
    }
}
