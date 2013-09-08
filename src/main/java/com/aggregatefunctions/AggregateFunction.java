package com.aggregatefunctions;

public interface AggregateFunction {
    void setPeriod(long beginTime, long endTime);
    void feed(long value);
    long result();
}
