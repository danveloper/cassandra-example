package com.hazelcast.webmonitor.newdatapoint;

public class Datapoint {
    public String metricName;
    public long timestampMs;
    public long value;
    public String cluster;
    public String member;
    public String id;
    public String company;

    public Datapoint() {
    }

    public Datapoint(Datapoint d){
        this(d.metricName,d.timestampMs,d.value,d.cluster,d.member,d.id,d.company);
    }

    public Datapoint(String metricName, long timestampMs, long value, String cluster, String member, String id, String company) {
        this.metricName = metricName;
        this.timestampMs = timestampMs;
        this.value = value;
        this.cluster = cluster;
        this.member = member;
        this.id = id;
        this.company = company;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Datapoint datapoint = (Datapoint) o;

        if (timestampMs != datapoint.timestampMs) return false;
        if (value != datapoint.value) return false;
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
        result = 31 * result + (int) (value ^ (value >>> 32));
        result = 31 * result + (cluster != null ? cluster.hashCode() : 0);
        result = 31 * result + (member != null ? member.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (company != null ? company.hashCode() : 0);
        return result;
    }

    @Override
    public String toString() {
        return "Datapoint{" +
                "metricName='" + metricName + '\'' +
                ", timestampMs=" + timestampMs +
                ", value=" + value +
                ", cluster='" + cluster + '\'' +
                ", member='" + member + '\'' +
                ", id='" + id + '\'' +
                ", company='" + company + '\'' +
                '}';
    }
}
