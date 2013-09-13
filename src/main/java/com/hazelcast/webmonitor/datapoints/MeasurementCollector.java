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

public class MeasurementCollector {

    private final DatapointRepository[] repositories;
    private final AtomicReference<MeasurementNode> measurementsRef = new AtomicReference<MeasurementNode>();
    private final MeasurementStrainProcessor[] processors;
    private final BlockingQueue<MeasurementStrain>[] queues;

    public MeasurementCollector(Cluster cluster, Keyspace keyspace, int[] rollupPeriods) {
        if (rollupPeriods.length == 0) {
            throw new IllegalArgumentException();
        }

        repositories = new DatapointRepository[rollupPeriods.length];
        processors = new MeasurementStrainProcessor[repositories.length];
        queues = new BlockingQueue[repositories.length];

        for (int k = 0; k < repositories.length; k++) {
            int rollupPeriodSeconds = rollupPeriods[k];
            if (rollupPeriodSeconds <= 0) {
                throw new IllegalArgumentException();
            }

            int rollupPeriodMs = rollupPeriodSeconds * 1000;
            DatapointRepository repository = new DatapointRepository(cluster, keyspace, "by_" + rollupPeriodSeconds + "_seconds", rollupPeriodMs);
            repositories[k] = repository;

            final BlockingQueue<MeasurementStrain> queue = new LinkedBlockingQueue<MeasurementStrain>();
            queues[k] = queue;

            final MeasurementStrainProcessor scheduleRunnable = new MeasurementStrainProcessor(repository);
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
        for(;;){
            MeasurementNode oldHead = measurementsRef.get();
            MeasurementNode newHead = new MeasurementNode(oldHead,measurement);
            if(measurementsRef.compareAndSet(oldHead, newHead)){
                return;
            }
        }
    }

    public void start() {
        for (int k = 0; k < processors.length; k++) {
            final BlockingQueue<MeasurementStrain> queue = queues[k];
            final MeasurementStrainProcessor processor = processors[k];

            new Thread() {
                public void run() {
                    try {
                        for (; ; ) {
                            MeasurementStrain command = queue.take();
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

                        MeasurementNode head = measurementsRef.getAndSet(null);

                        long startMs = System.currentTimeMillis();
                        MeasurementStrain processCommand = new MeasurementStrain(startMs, head);

                        for (BlockingQueue<MeasurementStrain> queue : queues) {
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
