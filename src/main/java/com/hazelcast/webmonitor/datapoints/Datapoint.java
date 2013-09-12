package com.hazelcast.webmonitor.datapoints;

public class Datapoint {
    public String metricName;
    public long timestampMs;

    public String cluster;
    public String member;
    public String id;
    public String company;

    public double minimum;
    public double maximum;
    public double avg;

    public Datapoint() {
    }

    public Datapoint(Datapoint datapoint){
        this(datapoint.metricName,datapoint.timestampMs,datapoint.cluster,datapoint.member,datapoint.id,datapoint.company,datapoint.minimum,datapoint.maximum,datapoint.avg);
    }

    public Datapoint(String metricName, long timestampMs, String cluster, String member, String id, String company, double minimum, double maximum, double avg) {
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

        if (Double.compare(datapoint.avg, avg) != 0) return false;
        if (Double.compare(datapoint.maximum, maximum) != 0) return false;
        if (Double.compare(datapoint.minimum, minimum) != 0) return false;
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
        int result;
        long temp;
        result = metricName != null ? metricName.hashCode() : 0;
        result = 31 * result + (int) (timestampMs ^ (timestampMs >>> 32));
        result = 31 * result + (cluster != null ? cluster.hashCode() : 0);
        result = 31 * result + (member != null ? member.hashCode() : 0);
        result = 31 * result + (id != null ? id.hashCode() : 0);
        result = 31 * result + (company != null ? company.hashCode() : 0);
        temp = Double.doubleToLongBits(minimum);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(maximum);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        temp = Double.doubleToLongBits(avg);
        result = 31 * result + (int) (temp ^ (temp >>> 32));
        return result;
    }
}
