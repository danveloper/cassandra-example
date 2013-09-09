package com.hazelcast.webmonitor.aggregatefunctions;

public class AggregateFunctionFactory {
    private final Class<? extends AggregateFunction> clazz;

    public AggregateFunctionFactory(Class<? extends AggregateFunction> clazz){
        this.clazz = clazz;
    }

    public AggregateFunction newAggregateFunction(){
        try {
            return clazz.newInstance();
        } catch (InstantiationException e) {
            throw new RuntimeException(e);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
