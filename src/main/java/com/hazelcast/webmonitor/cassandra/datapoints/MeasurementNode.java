package com.hazelcast.webmonitor.cassandra.datapoints;

import com.hazelcast.webmonitor.Measurement;

public class MeasurementNode {
    public final MeasurementNode next;
    public final Measurement measurement;

    public MeasurementNode(MeasurementNode next, Measurement measurement) {
        this.next = next;
        this.measurement = measurement;
    }
}
