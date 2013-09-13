package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.Measurement;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;


//based on the incoming time they could be placed in their own bucket.

public class DatapointCollector {

    private final DatapointRepository[] repositories;
    private final AtomicReference<List<Measurement>> dataPointsRef = new AtomicReference<List<Measurement>>(new Vector<Measurement>());
    private final long maximumRollupMs;

    public DatapointCollector(Cluster cluster, Keyspace keyspace, int[] rollupPeriods) {
        if (rollupPeriods.length == 0) {
            throw new IllegalArgumentException();
        }

        repositories = new DatapointRepository[rollupPeriods.length];
        long maximumRollupMs = Long.MIN_VALUE;

        for (int k = 0; k < repositories.length; k++) {
            int rollupPeriodSeconds = rollupPeriods[k];
            if (rollupPeriodSeconds <= 0) {
                throw new IllegalArgumentException();
            }

            int rollupPeriodMs = rollupPeriodSeconds * 1000;
            if (rollupPeriodMs > maximumRollupMs) {
                maximumRollupMs = rollupPeriodMs;
            }
            repositories[k] = new DatapointRepository(cluster, keyspace, "by_" + rollupPeriodSeconds + "_seconds", rollupPeriodMs);
        }
        this.maximumRollupMs = maximumRollupMs;
    }

    public DatapointRepository getRepository(int rollupPeriod) {
        for (DatapointRepository repo : repositories) {
            if (repo.getRollupPeriodMs() == rollupPeriod * 1000) {
                return repo;
            }
        }

        return null;
    }

    public void publish(Measurement measurement) {
        dataPointsRef.get().add(measurement);
    }

    public void start() {
        final DatapointRepositoryTask[] repositoryTasks = new DatapointRepositoryTask[repositories.length];
        for (int k = 0; k < repositoryTasks.length; k++) {
            DatapointRepository repository = repositories[k];
            DatapointRepositoryTask scheduleRunnable = new DatapointRepositoryTask(repository);
            repositoryTasks[k] = scheduleRunnable;
            new Thread(scheduleRunnable).start();
        }

        new Thread() {
            @Override
            public void run() {
                long sleepMs = 1000;
                try {
                    for (; ; ) {
                        doSleep(sleepMs);

                        List<Measurement> measurements = dataPointsRef.getAndSet(new Vector<Measurement>());


                        long startMs = System.currentTimeMillis();
                        ProcessCommand processCommand = new ProcessCommand(startMs, measurements);
                        for (DatapointRepositoryTask repositoryTask : repositoryTasks) {
                            repositoryTask.getMeasurementsQueue().add(processCommand);
                        }
                        long endMs = System.currentTimeMillis();
                        long durationMs = endMs - startMs;
                        sleepMs = 1000 - durationMs;
                        if (sleepMs < 0) {
                            //todo
                            System.out.println("Expired");
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }.start();
    }

    private static void doSleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
        }
    }
}
