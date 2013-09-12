package com.hazelcast.webmonitor.newdatapoint;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicReference;


//based on the incoming time they could be placed in their own bucket.

/**
 * todo:
 * - max aggregator has a memory leak since it will keep track of all datapoints
 * - no parallellisation
 * - merge min/max/avg into single value
 * - max aggregators items are never removed.. so also a potential OOME.
 */
public class DatapointCollector {

    private final NewDatapointRepository[] repositories;

    private final AtomicReference<List<Datapoint>> dataPointsRef = new AtomicReference<List<Datapoint>>(new Vector<Datapoint>());

    private final long maximumRollupMs;

    public DatapointCollector(Cluster cluster, Keyspace keyspace, int[] rollupPeriods) {
        if (rollupPeriods.length == 0) {
            throw new IllegalArgumentException();
        }

        repositories = new NewDatapointRepository[rollupPeriods.length];
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
            repositories[k] = new NewDatapointRepository(cluster, keyspace, "by_" + rollupPeriodSeconds + "_seconds", rollupPeriodMs);
        }
        this.maximumRollupMs = maximumRollupMs;
    }

    public NewDatapointRepository getRepository(int rollupPeriod) {
        for (NewDatapointRepository repo : repositories) {
            if (repo.getRollupPeriodMs() == rollupPeriod * 1000) {
                return repo;
            }
        }

        return null;
    }

    public void publish(Datapoint datapoint) {
        dataPointsRef.get().add(datapoint);
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
        long maximum = Long.MIN_VALUE;
        long minimum = Long.MAX_VALUE;
        long count;
        long total;
    }

    class Aggregator {

        private Datapoint template;
        private Node head;

        void publish(Datapoint datapoint, long timeMs) {
            if (template == null) {
                template = new Datapoint(datapoint);
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

        void aggregate(NewDatapointRepository repository, long timeMs) {
            long rollupPeriodMs = repository.getRollupPeriodMs();
            long maxTime = System.currentTimeMillis() - rollupPeriodMs;

            long maxvalue = Long.MIN_VALUE;
            long minvalue = Long.MAX_VALUE;
            int items = 0;
            long sum = 0;

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

            result.metricName = "max(" + template.metricName + ")";
            result.value = maxvalue;
            repository.insert(result);

            result.metricName = "min(" + template.metricName + ")";
            result.value = minvalue;
            repository.insert(result);

            result.value = items == 0 ? 0 : sum / items;
            result.metricName = "avg(" + template.metricName + ")";
            repository.insert(result);
        }
    }

    private Map<String, Aggregator> aggregators = new HashMap<String, Aggregator>();

    public void flush(long timeMs) {
        List<Datapoint> rawDatapoints = dataPointsRef.getAndSet(new Vector<Datapoint>());
        if (rawDatapoints.isEmpty()) {
            return;
        }

        for (Datapoint datapoint : rawDatapoints) {
            //Datapoint memberAgnosticDatapoint = new Datapoint(datapoint);
            //memberAgnosticDatapoint.member="";

            //Datapoint idAgnosticDatapoint = new Datapoint(datapoint);
            //idAgnosticDatapoint.id="";

            x(datapoint,timeMs);
            //x(memberAgnosticDatapoint);
            //x(idAgnosticDatapoint);
        }

        for (Aggregator aggregator : aggregators.values()) {
            for (NewDatapointRepository repository : repositories) {
                aggregator.aggregate(repository, timeMs);
            }
        }
    }

    private void x(Datapoint datapoint, long timeMs) {
        String key = id(datapoint);

        Aggregator calculator = aggregators.get(key);
        if (calculator == null) {
            calculator = new Aggregator();
            aggregators.put(key, calculator);
        }

        calculator.publish(datapoint,timeMs);
    }

    private String id(Datapoint datapoint) {
        return datapoint.company + "!" + datapoint.cluster + "!" + datapoint.member + "!" + datapoint.id + "!" + datapoint.metricName;
    }


}
