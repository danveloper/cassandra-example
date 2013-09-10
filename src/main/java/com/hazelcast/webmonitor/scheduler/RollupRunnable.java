package com.hazelcast.webmonitor.scheduler;

import com.hazelcast.webmonitor.repositories.CompanyRepository;
import com.hazelcast.webmonitor.repositories.DatapointRepository;
import org.apache.log4j.Logger;

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
    private final Class<? extends AbstractRollupRunnable> clazz;
    private final ExecutorService executorService;
    private final CompanyRepository companyRepository;
    private long startMs = System.currentTimeMillis();

    public RollupRunnable(DatapointRepository source, DatapointRepository target, String description,
                          Class<? extends AbstractRollupRunnable> clazz, ExecutorService executorService, CompanyRepository companyRepository) {
        this.source = source;
        this.target = target;
        this.description = description;
        this.clazz = clazz;
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
            for (String company : companyRepository.getCompanyNames()) {
                AbstractRollupRunnable task = clazz.newInstance();
                task.source = source;
                task.target = target;
                task.company = company;
                task.startMs = startMs;
                task.endMs = endMs;
                Future<?> future = executorService.submit(task);
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

            System.out.println("Finished " + description + " companies:" + futures.size());
        } catch (Throwable t) {
            logger.warn("Failed to compact " + description, t);
        }

        startMs = endMs;
    }
}
