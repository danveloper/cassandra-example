package com.aggregatefunctions;

public class SumFunction implements AggregateFunction{
    private long result;

    @Override
    public void setPeriod(long beginTime, long endTime) {
    }

    @Override
    public void feed(long value) {
        this.result +=value;
    }

    @Override
    public long result() {
        return result;
    }
}
