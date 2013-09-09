package com;

import com.aggregatefunctions.AggregateFunctionFactory;
import com.repositories.CompanyRepository;
import com.repositories.DatapointRepository;
import com.repositories.RollupSchedulerRepository;

import java.util.concurrent.*;

public class RollupScheduler {

    private final ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(10);
    private final ExecutorService executor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());
    private final CompanyRepository companyRepository;
    private final RollupSchedulerRepository schedulerRepository;

    public RollupScheduler(RollupSchedulerRepository schedulerRepository, CompanyRepository companyRepository) {
        this.companyRepository = companyRepository;
        this.schedulerRepository = schedulerRepository;
    }

    public void schedule(DatapointRepository source, DatapointRepository target, AggregateFunctionFactory functionFactory, int periodMs) {
        RollupRunnable rollupRunnable = new RollupRunnable(
                source,
                target,
                "average 1 second",
                functionFactory,
                executor, companyRepository);

        scheduler.scheduleAtFixedRate(rollupRunnable, 0, periodMs, TimeUnit.MILLISECONDS);
    }
}
