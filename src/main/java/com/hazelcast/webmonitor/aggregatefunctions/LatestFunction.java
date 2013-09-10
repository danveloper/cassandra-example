package com.hazelcast.webmonitor.aggregatefunctions;

/**
 * Returns the latest value.
 */
public class LatestFunction implements AggregateFunction{
    long value;

    @Override
    public void setPeriod(long beginTime, long endTime) {
    }

    @Override
    public void feed(long value) {
        this.value = value;
    }

    @Override
    public long result() {
        return value;
    }
}
