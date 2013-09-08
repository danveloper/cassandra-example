package com;

import com.aggregatefunctions.AggregateFunctionFactory;

import java.util.concurrent.*;

public class RollupScheduler {

    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(10);
    private final ExecutorService executor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    private final CustomersRepository customersRepository;
    private final SchedulerRepository schedulerRepository;

    public RollupScheduler(SchedulerRepository schedulerRepository, CustomersRepository customersRepository) {
        this.customersRepository = customersRepository;
        this.schedulerRepository = schedulerRepository;
    }

    public void schedule(DatapointRepository source, DatapointRepository target, AggregateFunctionFactory functionFactory, int periodMs) {
        RollupRunnable rollupRunnable = new RollupRunnable(
                source,
                target,
                "average 1 second",
                functionFactory,
                executor, customersRepository);

        scheduler.scheduleAtFixedRate(rollupRunnable, 0, periodMs, TimeUnit.MILLISECONDS);
    }
}
