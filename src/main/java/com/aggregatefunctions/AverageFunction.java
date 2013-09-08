package com.aggregatefunctions;

public class AverageFunction implements AggregateFunction {
    private int count;
    private long sum;
    @Override
    public void setPeriod(long beginTime, long endTime) {

    }

    @Override
    public void feed(long value) {
        count++;
        sum+=value;
    }

    @Override
    public long result() {
        return count==0?0:sum/count;
    }
}
