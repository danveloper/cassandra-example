package com.hazelcast.webmonitor.model;

public class Datapoint {
    public String metricName;
    public long timestampMs;

    public String cluster;
    public String member;
    public String id;
    public String company;

    public long minimum;
    public long maximum;
    public long avg;

    public Datapoint() {
    }

    public Datapoint(Datapoint datapoint){
        this(datapoint.metricName,datapoint.timestampMs,datapoint.cluster,datapoint.member,datapoint.id,datapoint.company,datapoint.minimum,datapoint.maximum,datapoint.avg);
    }

    public Datapoint(String metricName, long timestampMs, String cluster, String member, String id, String company, long minimum, long maximum, long avg) {
        this.metricName = metricName;
        this.timestampMs = timestampMs;
        this.cluster = cluster;
        this.member = member;
        this.id = id;
        this.company = company;
        this.minimum = minimum;
        this.maximum = maximum;
        this.avg = avg;
    }

    @Override
    public String toString() {
        return "Datapoint{" +
                "metricName='" + metricName + '\'' +
                ", timestampMs=" + timestampMs +
                ", cluster='" + cluster + '\'' +
                ", member='" + member + '\'' +
                ", id='" + id + '\'' +
                ", company='" + company + '\'' +
                ", minimum=" + minimum +
                ", maximum=" + maximum +
                ", avg=" + avg +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Datapoint datapoint = (Datapoint) o;

        if (avg != datapoint.avg) return false;
        if (maximum != datapoint.maximum) return false;
        if (minimum != datapoint.minimum) return false;
        if (timestampMs != datapoint.timestampMs) return false;
        if (cluster != null ? !cluster.equals(datapoint.cluster) : datapoint.cluster != null) return false;
        if (company != null ? !company.equals(datapoint.company) : datapoint.company != null) return false;
        if (id != null ? !id.equals(datapoint.id) : datapoint.id != null) return false;
        if (member != null ? !member.equals(datapoint.member) : datapoint.member != null) return false;
        if (metricName != null ? !metricName.equals(datapoint.metricName) : datapoint.metricName != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = metricName != null ? metricName.hashCode() : 0;
        result = 31 * result + (int) (timestampMs ^ (timestampMs >>> 32));
        result = 31 * result + (cluster != null ? cluster.hashCode() : 0);
        result = 31 * result + (member != null ? member.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (company != null ? company.hashCode() : 0);
        result = 31 * result + (int) (minimum ^ (minimum >>> 32));
        result = 31 * result + (int) (maximum ^ (maximum >>> 32));
        result = 31 * result + (int) (avg ^ (avg >>> 32));
        return result;
    }
}
