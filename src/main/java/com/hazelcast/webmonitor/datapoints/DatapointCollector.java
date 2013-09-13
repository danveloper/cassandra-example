package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.Measurement;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import java.util.List;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicReference;


//based on the incoming time they could be placed in their own bucket.

public class DatapointCollector {

    private final DatapointRepository[] repositories;
    private final AtomicReference<List<Measurement>> measurementsRef = new AtomicReference<List<Measurement>>(new Vector<Measurement>());
    final DatapointRepositoryProcessor[] processors;
    final BlockingQueue<ProcessCommand>[] queues;

    public DatapointCollector(Cluster cluster, Keyspace keyspace, int[] rollupPeriods) {
        if (rollupPeriods.length == 0) {
            throw new IllegalArgumentException();
        }

        repositories = new DatapointRepository[rollupPeriods.length];
        processors = new DatapointRepositoryProcessor[repositories.length];
        queues = new BlockingQueue[repositories.length];

        for (int k = 0; k < repositories.length; k++) {
            int rollupPeriodSeconds = rollupPeriods[k];
            if (rollupPeriodSeconds <= 0) {
                throw new IllegalArgumentException();
            }

            int rollupPeriodMs = rollupPeriodSeconds * 1000;
            DatapointRepository repository = new DatapointRepository(cluster, keyspace, "by_" + rollupPeriodSeconds + "_seconds", rollupPeriodMs);
            repositories[k] = repository;

            final BlockingQueue<ProcessCommand> queue = new LinkedBlockingQueue<ProcessCommand>();
            queues[k] = queue;

            final DatapointRepositoryProcessor scheduleRunnable = new DatapointRepositoryProcessor(repository);
            processors[k] = scheduleRunnable;
        }
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
        measurementsRef.get().add(measurement);
    }

    public void start() {
        for (int k = 0; k < processors.length; k++) {
            final BlockingQueue<ProcessCommand> queue = queues[k];
            final DatapointRepositoryProcessor processor = processors[k];

            new Thread() {
                public void run() {
                    try {
                        for (; ; ) {
                            ProcessCommand command = queue.take();
                            processor.process(command);
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }
            }.start();
        }

        new Thread() {
            @Override
            public void run() {
                long sleepMs = 1000;
                try {
                    for (; ; ) {
                        doSleep(sleepMs);

                        List<Measurement> measurements = measurementsRef.getAndSet(new Vector<Measurement>());

                        long startMs = System.currentTimeMillis();
                        ProcessCommand processCommand = new ProcessCommand(startMs, measurements);

                        for (BlockingQueue<ProcessCommand> queue : queues) {
                            queue.add(processCommand);
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
