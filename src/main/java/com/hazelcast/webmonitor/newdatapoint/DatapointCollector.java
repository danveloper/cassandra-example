package com.hazelcast.webmonitor.newdatapoint;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import java.util.*;
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
        long maximumRollupMs = Long.MAX_VALUE;

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

    public void start() {
        new Thread() {
            public void run() {
                long sleepMs = 1000;
                try {
                    for (; ; ) {
                        dosleep(sleepMs);

                        long startMs = System.currentTimeMillis();
                        flush(startMs);
                        long endMs = System.currentTimeMillis();
                        long durationMs = endMs-startMs;
                        sleepMs = 1000-durationMs;
                        if(sleepMs<0){
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

    private static void dosleep(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            e.printStackTrace();  //To change body of catch statement use File | Settings | File Templates.
        }
    }

    class Aggregator {

        private Datapoint template;
        private LinkedList<Datapoint> list = new LinkedList();

        void publish(Datapoint datapoint) {
            if (template == null) {
                template = new Datapoint(datapoint);
            }
            list.offerFirst(datapoint);
        }

        Datapoint calcMax(long timestampMs, long rollupPeriodMs) {
            Datapoint result = new Datapoint(template);
            long maxTime = System.currentTimeMillis() - rollupPeriodMs;

            long maxvalue = Long.MIN_VALUE;
            for (int k = 0; k < list.size(); k++) {
                Datapoint d = list.get(k);

                if (d.timestampMs < maxTime) {
                    break;
                }

                if (d.value > maxvalue) {
                    maxvalue = d.value;
                }
            }

            result.metricName = "max(" + template.metricName + ")";
            result.timestampMs = rollupPeriodMs * (timestampMs / rollupPeriodMs);
            result.value = maxvalue;
            return result;
        }

        Datapoint calcMin(long timestampMs, long rollupPeriodMs) {
            Datapoint result = new Datapoint(template);
            long maxTime = System.currentTimeMillis() - rollupPeriodMs;

            long minvalue = Long.MAX_VALUE;
            for (int k = 0; k < list.size(); k++) {
                Datapoint d = list.get(k);

                if (d.timestampMs < maxTime) {
                    break;
                }

                if (d.value < minvalue) {
                    minvalue = d.value;
                }
            }

            result.metricName = "min(" + template.metricName + ")";
            result.timestampMs = rollupPeriodMs * (timestampMs / rollupPeriodMs);
            result.value = minvalue;
            return result;
        }

        Datapoint calcAvg(long timestampMs, long rollupPeriodMs) {
            Datapoint result = new Datapoint(template);
            long maxTime = System.currentTimeMillis() - rollupPeriodMs;

            int items = 0;
            long sum = 0;
            for (int k = 0; k < list.size(); k++) {
                Datapoint d = list.get(k);

                if (d.timestampMs < maxTime) {
                    break;
                }
                items++;
                sum += d.value;

            }

            result.metricName = "avg(" + template.metricName + ")";
            result.timestampMs = rollupPeriodMs * (timestampMs / rollupPeriodMs);
            result.value = items == 0 ? 0 : sum / items;
            return result;
        }
    }

    private Map<String, Aggregator> aggregators = new HashMap<String, Aggregator>();

    public void flush(long timeStamp) {
        List<Datapoint> rawDatapoints = dataPointsRef.getAndSet(new Vector<Datapoint>());
        if (rawDatapoints.isEmpty()) {
            return;
        }

        for (Datapoint datapoint : rawDatapoints) {
            //Datapoint memberAgnosticDatapoint = new Datapoint(datapoint);
            //memberAgnosticDatapoint.member="";

            //Datapoint idAgnosticDatapoint = new Datapoint(datapoint);
            //idAgnosticDatapoint.id="";

            x(datapoint);
            //x(memberAgnosticDatapoint);
            //x(idAgnosticDatapoint);
        }

        for (Aggregator collector : aggregators.values()) {
            for (NewDatapointRepository repository : repositories) {
                repository.insert(collector.calcMax(timeStamp, repository.getRollupPeriodMs()));
                repository.insert(collector.calcMin(timeStamp, repository.getRollupPeriodMs()));
                repository.insert(collector.calcAvg(timeStamp, repository.getRollupPeriodMs()));
            }
        }
    }

    private void x(Datapoint datapoint) {
        String key = id(datapoint);

        Aggregator calculator = aggregators.get(key);
        if (calculator == null) {
            calculator = new Aggregator();
            aggregators.put(key, calculator);
        }

        calculator.publish(datapoint);
    }

    private String id(Datapoint datapoint) {
        return datapoint.company + "!" + datapoint.cluster + "!" + datapoint.member + "!" + datapoint.id + "!" + datapoint.metricName;
    }

    public void publish(Datapoint datapoint) {
        dataPointsRef.get().add(datapoint);
    }
}
