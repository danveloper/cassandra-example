package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.Measurement;

import java.util.List;

class ProcessCommand {
    private final long timeMs;
    private final List<Measurement> measurements;

    ProcessCommand(long timeMs, List<Measurement> measurements) {
        this.timeMs = timeMs;
        this.measurements = measurements;
    }

    public long getTimeMs() {
        return timeMs;
    }

    public List<Measurement> getMeasurements() {
        return measurements;
    }
}
