package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.Measurement;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;


//based on the incoming time they could be placed in their own bucket.

/**
 */
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
        new Thread() {
            public void run() {
                long sleepMs = 1000;
                try {
                    for (; ; ) {
                        doSleep(sleepMs);

                        long startMs = System.currentTimeMillis();
                        flush(startMs);
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

    static class Node {
        Node next;
        long timestampMs;
        double maximum = Double.MIN_VALUE;
        double minimum = Double.MAX_VALUE;
        long count;
        double total;
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

            head.total += datapoint.value;
            head.count++;
        }

        void aggregate(DatapointRepository repository, long timeMs) {
            long rollupPeriodMs = repository.getRollupPeriodMs();
            long maxTime = System.currentTimeMillis() - rollupPeriodMs;

            double maxvalue = Long.MIN_VALUE;
            double minvalue = Long.MAX_VALUE;
            int items = 0;
            double sum = 0;

            Node node = head;
            Node previous = null;
            while (node != null) {
                if (node.timestampMs < maxTime) {
                    if (timeMs - node.timestampMs > maximumRollupMs) {
                        if (previous == null) {
                            head = null;
                        } else {
                            previous.next = null;
                        }
                    }
                    break;
                }

                if (node.maximum > maxvalue) {
                    maxvalue = node.maximum;
                }

                if (node.minimum < minvalue) {
                    minvalue = node.minimum;
                }

                items+=node.count;
                sum += node.total;
                previous = node;
                node = node.next;
            }

            Datapoint result = new Datapoint(template);
            result.timestampMs = timeMs;

            result.metricName = template.metricName;
            result.maximum = maxvalue;
            result.minimum = minvalue;
            result.average = items == 0 ? 0 : sum / items;
            repository.insert(result);
        }
    }

    private Map<String, Aggregator> aggregators = new HashMap<String, Aggregator>();

    public void flush(long timeMs) {
        List<Measurement> rawDatapoints = dataPointsRef.getAndSet(new Vector<Measurement>());
        if (rawDatapoints.isEmpty()) {
            return;
        }

        for (Measurement datapoint : rawDatapoints) {
            //Datapoint memberAgnosticDatapoint = new Datapoint(datapoint);
            //memberAgnosticDatapoint.member="";

            //Datapoint idAgnosticDatapoint = new Datapoint(datapoint);
            //idAgnosticDatapoint.id="";

            x(datapoint,timeMs);
            //x(memberAgnosticDatapoint);
            //x(idAgnosticDatapoint);
        }

        for (Aggregator aggregator : aggregators.values()) {
            for (DatapointRepository repository : repositories) {
                aggregator.aggregate(repository, timeMs);
            }
        }
    }

    private void x(Measurement datapoint, long timeMs) {
        String key = id(datapoint);

        Aggregator calculator = aggregators.get(key);
        if (calculator == null) {
            calculator = new Aggregator();
            aggregators.put(key, calculator);
        }

        calculator.publish(datapoint,timeMs);
    }

    private String id(Measurement datapoint) {
        return datapoint.company + "!" + datapoint.cluster + "!" + datapoint.member + "!" + datapoint.id + "!" + datapoint.metricName;
    }


}
