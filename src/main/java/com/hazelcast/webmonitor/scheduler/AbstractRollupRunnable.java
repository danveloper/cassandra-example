package com.hazelcast.webmonitor.scheduler;

import com.hazelcast.webmonitor.repositories.DatapointRepository;

/**
 * Does a rollup for a single company, for a single sensor, within a given time period.
 */
public abstract class AbstractRollupRunnable implements Runnable {

    public long startMs;
    public long endMs;
    public DatapointRepository source;
    public DatapointRepository target;
    public String company;
}
