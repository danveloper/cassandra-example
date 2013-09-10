package com.hazelcast.webmonitor.aggregatefunctions;

public interface AggregateFunction {
    void setPeriod(long beginTime, long endTime);
    //the values will be fed in chronological order.
    void feed(long value);
    long result();
}
