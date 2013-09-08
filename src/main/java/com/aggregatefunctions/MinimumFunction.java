package com.aggregatefunctions;

public class MinimumFunction implements AggregateFunction {

    private long minimum = Long.MAX_VALUE;

    @Override
    public void setPeriod(long beginTime, long endTime) {
    }

    @Override
    public void feed(long value) {
        if (value < minimum) {
            minimum = value;
        }
    }

    @Override
    public long result() {
        return minimum;
    }
}