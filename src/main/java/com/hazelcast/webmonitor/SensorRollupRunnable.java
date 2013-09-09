package com.hazelcast.webmonitor;

import com.hazelcast.webmonitor.aggregatefunctions.AggregateFunction;
import com.eaio.uuid.UUID;
import com.hazelcast.webmonitor.repositories.DatapointRepository;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.log4j.Logger;

import java.util.Iterator;

/**
 * Does a rollup for a single customer, for a single sensor, within a given time period.
 */
public class SensorRollupRunnable implements Runnable {

    private final static Logger logger = Logger.getLogger(SensorRollupRunnable.class);

    private final String sensor;
    private final long startMs;
    private final long endMs;
    private final String customer;
    private final AggregateFunction function;
    private final DatapointRepository source;
    private final DatapointRepository target;

    public SensorRollupRunnable(String customer, String sensor, long startMs, long endMs, AggregateFunction function,
                                DatapointRepository source, DatapointRepository target) {
        this.function = function;
        this.sensor = sensor;
        this.startMs = startMs;
        this.endMs = endMs;
        this.customer = customer;
        this.source = source;
        this.target = target;
    }

    @Override
    public void run() {
        try {
            function.setPeriod(startMs, endMs);

            Iterator<HCounterColumn<UUID>> iterator = source.dataPointIterator(customer, sensor, startMs, endMs);

            int count = 0;
            for (; iterator.hasNext(); ) {
                HCounterColumn<UUID> columns = iterator.next();
                long value = columns.getValue();
                function.feed(value);
                count++;
            }

            logger.info("compressed "+sensor+" count:"+count);


            target.insert(customer, sensor, startMs,  function.result());
        } catch (Throwable e) {
            logger.warn("Failed to do a rollup", e);
        }
    }
}
