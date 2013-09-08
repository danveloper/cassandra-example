package com.aggregatefunctions;

public class MaximumFunction implements AggregateFunction {

    private long maximum = Long.MIN_VALUE;

    @Override
    public void setPeriod(long beginTime, long endTime) {
    }

    @Override
    public void feed(long value) {
        if (value > maximum) {
            maximum = value;
        }
    }

    @Override
    public long result() {
        return maximum;
    }
}
