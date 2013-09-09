package com;

import com.aggregatefunctions.AggregateFunctionFactory;
import com.repositories.CompanyRepository;
import com.repositories.DatapointRepository;
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
    private final CompanyRepository companyRepository;
    private long startMs = System.currentTimeMillis();

    public RollupRunnable(DatapointRepository source, DatapointRepository target, String description,
                          AggregateFunctionFactory functionFactory, ExecutorService executorService, CompanyRepository companyRepository) {
        this.source = source;
        this.target = target;
        this.description = description;
        this.functionFactory = functionFactory;
        this.executorService = executorService;
        this.companyRepository = companyRepository;
    }

    @Override
    public void run() {

        long endMs = System.currentTimeMillis();
        if (endMs <= startMs) {
            endMs = startMs + 1;
        }
        try {

            List<Future> futures = new LinkedList<Future>();

            for (String customer : companyRepository.getCompanyNames()) {
                for (Iterator<String> it = source.sensorNameIterator(customer, startMs, endMs); it.hasNext(); ) {
                    String sensor = it.next();
                    SensorRollupRunnable task = new SensorRollupRunnable(customer, sensor, startMs, endMs, functionFactory.newAggregateFunction(), source, target);
                    Future<?> future = executorService.submit(task);
                    futures.add(future);
                }
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

            System.out.println("Finished compacting "+description+" sensors:"+futures.size());
        } catch (Throwable t) {
            logger.warn("Failed to compact " + description, t);
        }

        startMs = endMs;
    }
}
