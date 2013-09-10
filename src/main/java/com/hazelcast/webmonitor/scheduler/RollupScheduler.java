package com.hazelcast.webmonitor.scheduler;

import com.hazelcast.webmonitor.repositories.CompanyRepository;
import com.hazelcast.webmonitor.repositories.DatapointRepository;
import com.hazelcast.webmonitor.repositories.RollupSchedulerRepository;

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

    public void schedule(String desc, DatapointRepository source, DatapointRepository target, Class<? extends AbstractRollupRunnable> clazz, int periodMs) {
        RollupRunnable rollupRunnable = new RollupRunnable(
                source,
                target,
                desc,
                clazz,
                executor, companyRepository);

        scheduler.scheduleAtFixedRate(rollupRunnable, 0, periodMs, TimeUnit.MILLISECONDS);
    }
}
