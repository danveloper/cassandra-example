package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.Measurement;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
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
                        ProcessCommand processCommand = new ProcessCommand();
                        processCommand.measurements = measurements;
                        processCommand.timeMs = startMs;
                        for (DatapointRepositoryTask repositoryTask : repositoryTasks) {
                            repositoryTask.measurementsQueue.add(processCommand);
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

    private class ProcessCommand {
        long timeMs;
        List<Measurement> measurements;
    }

    private class DatapointRepositoryTask implements Runnable {
        private final DatapointRepository repository;
        private final Map<String, Aggregator> aggregators = new HashMap<String, Aggregator>();
        private final BlockingQueue<ProcessCommand> measurementsQueue = new LinkedBlockingQueue<ProcessCommand>();

        private DatapointRepositoryTask(DatapointRepository repository) {
            this.repository = repository;
        }

        @Override
        public void run() {
            try {
                for (; ; ) {
                    ProcessCommand command = measurementsQueue.take();
                    flush(command.measurements, command.timeMs);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        public void flush(List<Measurement> measurements, long timeMs) {
            if (measurements.isEmpty()) {
                return;
            }

            for (Measurement datapoint : measurements) {
                //Datapoint memberAgnosticDatapoint = new Datapoint(datapoint);
                //memberAgnosticDatapoint.member="";

                //Datapoint idAgnosticDatapoint = new Datapoint(datapoint);
                //idAgnosticDatapoint.id="";

                x(datapoint, timeMs);
                //x(memberAgnosticDatapoint);
                //x(idAgnosticDatapoint);
            }

            for (Aggregator aggregator : aggregators.values()) {
                aggregator.aggregate(repository, timeMs);
            }
        }

        private void x(Measurement datapoint, long timeMs) {
            String key = id(datapoint);

            Aggregator calculator = aggregators.get(key);
            if (calculator == null) {
                calculator = new Aggregator();
                aggregators.put(key, calculator);
            }

            calculator.publish(datapoint, timeMs);
        }
    }

    private static void doSleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
        }
    }

    static class Node {
        Node next;
        long timestampMs;
        double maximum = Double.MIN_VALUE;
        double minimum = Double.MAX_VALUE;
        long count;
        double sum;
    }

    class Aggregator {

        private Datapoint template;
        private Node head;

        void publish(Measurement datapoint, long timeMs) {
            if (template == null) {
                template = new Datapoint();
                template.metricName = datapoint.metricName;
                template.member = datapoint.member;
                template.cluster = datapoint.cluster;
                template.company = datapoint.company;
                template.id = datapoint.id;
            }

            if (head == null) {
                head = new Node();
                head.timestampMs = timeMs;
            } else if (head.timestampMs != timeMs) {
                Node node = new Node();
                node.timestampMs = timeMs;
                node.next = head;
                head = node;
            }

            if (datapoint.value > head.maximum) {
                head.maximum = datapoint.value;
            }

            if (datapoint.value < head.minimum) {
                head.minimum = datapoint.value;
            }

            head.sum += datapoint.value;
            head.count++;
        }

        void aggregate(DatapointRepository repository, long timeMs) {
            long rollupPeriodMs = repository.getRollupPeriodMs();
            int seconds = ((int) rollupPeriodMs / 1000) + 1;

            double maxvalue = Long.MIN_VALUE;
            double minvalue = Long.MAX_VALUE;
            int items = 0;
            double sum = 0;

            Node node = head;
            Node previous = null;
            Node tail = null;

            int k = 0;
            while (node != null) {
                if (k == seconds) {
                    if (timeMs - node.timestampMs > maximumRollupMs) {
                        if (previous == null) {
                            head = null;
                        } else {
                            previous.next = null;
                        }
                    }
                    break;
                }

                tail = node;

                if (node.maximum > maxvalue) {
                    maxvalue = node.maximum;
                }

                if (node.minimum < minvalue) {
                    minvalue = node.minimum;
                }

                items += node.count;
                sum += node.sum;
                previous = node;
                node = node.next;
                k++;
            }

            Datapoint result = new Datapoint(template);
            result.timestampMs = timeMs;
            result.metricName = template.metricName;
            result.maximum = maxvalue;
            result.minimum = minvalue;
            result.average = items == 0 ? 0 : sum / items;

            long durationMs = timeMs - tail.timestampMs;
            double difference = (head.sum / head.count) - (tail.sum / tail.count);
            result.velocity = durationMs == 0 ? 0 : (1000 * difference) / durationMs;

            repository.insert(result);
        }
    }


    private String id(Measurement datapoint) {
        return datapoint.company + "!" + datapoint.cluster + "!" + datapoint.member + "!" + datapoint.id + "!" + datapoint.metricName;
    }
}
