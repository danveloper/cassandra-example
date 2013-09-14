package com.hazelcast.webmonitor.cassandra.datapoints;

import com.hazelcast.webmonitor.Measurement;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class MeasurementCollector {

    private final DatapointRepository[] repositories;
    private final Partition[] partitions;
    private final Executor executor;

    public MeasurementCollector(Cluster cluster, Keyspace keyspace, int[] rollupPeriods, int partitionCount, Executor executor) {
        if (rollupPeriods.length == 0) {
            throw new IllegalArgumentException("rollupPeriods.length must be larger than 0");
        }

        if (partitionCount < 1) {
            throw new IllegalArgumentException("partitionCount must be larger than 0");
        }

        this.executor = executor;
        this.repositories = new DatapointRepository[rollupPeriods.length];
        for (int k = 0; k < repositories.length; k++) {
            int rollupPeriodSeconds = rollupPeriods[k];
            if (rollupPeriodSeconds <= 0) {
                throw new IllegalArgumentException();
            }

            int rollupPeriodMs = rollupPeriodSeconds * 1000;
            String columnFamilyName = "by_" + rollupPeriodSeconds + "_seconds";
            DatapointRepository repository = new DatapointRepository(cluster, keyspace, columnFamilyName, rollupPeriodMs);
            repositories[k] = repository;
        }

        this.partitions = new Partition[partitionCount];
        for (int k = 0; k < partitionCount; k++) {
            partitions[k] = new Partition();
        }
    }

    public DatapointRepository getRepository(int rollupPeriodSeconds) {
        if (rollupPeriodSeconds < 1) {
            throw new IllegalArgumentException("rollupPeriodSecond should be larger than 0");
        }

        for (DatapointRepository repo : repositories) {
            if (repo.getRollupPeriodMs() == rollupPeriodSeconds * 1000) {
                return repo;
            }
        }

        return null;
    }

    public void start() {
        for (Partition partition : partitions) {
            new Thread(partition).start();
        }
    }

    public void publish(Measurement measurement) {
        if (measurement == null) {
            throw new NullPointerException("Measurement can't be null");
        }

        int hash = (measurement.company + measurement.cluster + measurement.metricName).hashCode();
        if (hash == Integer.MIN_VALUE) {
            hash = Integer.MAX_VALUE;
        } else if (hash < 0) {
            hash = -hash;
        }


        int index = hash % partitions.length;
        Partition partition = partitions[index];
        partition.publish(measurement);
    }

    private class Partition implements Runnable {

        private final AtomicReference<MeasurementNode> measurementsRef = new AtomicReference<MeasurementNode>();
        private final Runner[] runners;

        public Partition() {
            runners = new Runner[repositories.length];
            for (int k = 0; k < repositories.length; k++) {
                MeasurementStrainProcessor processor = new MeasurementStrainProcessor(repositories[k]);
                runners[k] = new Runner(processor);
            }
        }

        @Override
        public void run() {
            long sleepMs = 1000;
            try {
                for (; ; ) {
                    doSleep(sleepMs);

                    MeasurementNode head = measurementsRef.getAndSet(null);

                    long startMs = System.currentTimeMillis();

                    Task task = new Task(startMs, head);
                    for (Runner runner : runners) {
                        runner.dosomething(task);
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

        private void publish(Measurement measurement) {
            for (; ; ) {
                MeasurementNode oldHead = measurementsRef.get();
                MeasurementNode newHead = new MeasurementNode(oldHead, measurement);
                if (measurementsRef.compareAndSet(oldHead, newHead)) {
                    return;
                }
            }
        }
    }

    private static class Task {
        private final long timeMs;
        private final MeasurementNode head;

        private Task(long timeMs, MeasurementNode head) {
            this.timeMs = timeMs;
            this.head = head;
        }
    }

    private class Runner implements Runnable {
        private final MeasurementStrainProcessor processor;
        private final AtomicBoolean scheduled = new AtomicBoolean(false);
        private final BlockingQueue<Task> queue = new LinkedBlockingQueue<Task>();

        private Runner(MeasurementStrainProcessor processor) {
            this.processor = processor;
        }

        public void dosomething(Task task) {
            queue.add(task);

            if (scheduled.get()) {
                return;
            }

            if (!scheduled.compareAndSet(false, true)) {
                return;
            }

            executor.execute(this);
        }

        @Override
        public void run() {
            try {
                try {
                    Task task = queue.remove();
                    processor.process(task.head, task.timeMs);
                } catch (Throwable t) {
                    t.printStackTrace();
                }

                if (queue.size() > 0) {
                    executor.execute(this);
                }

                scheduled.set(false);

                if (queue.size() == 0) {
                    return;
                }

                if (scheduled.get()) {
                    return;
                }

                if (!scheduled.compareAndSet(false, true)) {
                    return;
                }

                executor.execute(this);
            } catch (Throwable t) {
                t.printStackTrace();
            }
        }
    }

    private static void doSleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
        }
    }
}
