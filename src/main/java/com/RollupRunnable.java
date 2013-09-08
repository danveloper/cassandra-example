package com;

import com.aggregatefunctions.AggregateFunction;
import com.aggregatefunctions.AggregateFunctionFactory;
import com.eaio.uuid.UUID;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.beans.HColumn;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class RollupRunnable implements Runnable {

    private final static Logger logger = Logger.getLogger(RollupRunnable.class);

    private final DatapointRepository source;
    private final DatapointRepository target;
    private final String description;
    private final AggregateFunctionFactory functionFactory;
    private final ExecutorService executorService;
    private long startMs = System.currentTimeMillis();

    public RollupRunnable(DatapointRepository source, DatapointRepository target, String description,
                          AggregateFunctionFactory functionFactory, ExecutorService executorService) {
        this.source = source;
        this.target = target;
        this.description = description;
        this.functionFactory = functionFactory;
        this.executorService = executorService;
    }

    @Override
    public void run() {
        logger.info("Compacting " + description);

        long endMs = System.currentTimeMillis();
        if(endMs<=startMs){
            endMs = startMs+1;
        }
        try {

            List<Future> futures = new LinkedList<Future>();
            for (Iterator<String> it = source.sensorNameIterator(startMs, endMs); it.hasNext(); ) {
                String sensor = it.next();
                Future<?> future = executorService.submit(new ProcessRunnable(sensor, startMs, endMs));
                futures.add(future);
            }

            for (Future future : futures) {
                try {
                    future.get();
                } catch (InterruptedException e) {
                    logger.warn(e.getMessage(), e);
                } catch (ExecutionException e) {
                    logger.warn(e.getMessage(), e);
                }
            }
        } catch (Throwable t) {
            logger.warn("Failed to compact " + description, t);
        }

        startMs = endMs;
    }

    private class ProcessRunnable implements Runnable {
        private final String sensor;
        private final long startMs;
        private final long endMs;

        private ProcessRunnable(String sensor, long startMs, long endMs) {
            this.sensor = sensor;
            this.startMs = startMs;
            this.endMs = endMs;
        }

        @Override
        public void run() {
            try {
                AggregateFunction function = functionFactory.newAggregateFunction();
                function.setPeriod(this.startMs, endMs);

                ColumnSliceIterator<String, UUID, String> iterator = source.dataPointIterator(sensor, startMs, endMs);

                for (; iterator.hasNext(); ) {
                    HColumn<UUID, String> columns = iterator.next();
                    long value = new Long(columns.getValue());
                    function.feed(value);
                }

                target.update(sensor, startMs, "" + function.result());
            } catch (Throwable e) {
                logger.warn("Failed to compact", e);
            }
        }
    }
}
