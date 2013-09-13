package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.Measurement;

class MeasurementStrain {
    private final long timeMs;
    private final MeasurementNode head;

    MeasurementStrain(long timeMs, Measurement measurement){
        this(timeMs,  new MeasurementNode(null, measurement));
    }

    MeasurementStrain(long timeMs, MeasurementNode head) {
        this.timeMs = timeMs;
        this.head = head;
    }

    public long getTimeMs() {
        return timeMs;
    }

    MeasurementNode getHead() {
        return head;
    }
}
