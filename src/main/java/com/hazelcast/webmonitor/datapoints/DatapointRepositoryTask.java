package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.Measurement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

class DatapointRepositoryTask implements Runnable {
    private final DatapointRepository repository;
    private final Map<String, Aggregator> aggregators = new HashMap<String, Aggregator>();
    private final BlockingQueue<ProcessCommand> measurementsQueue = new LinkedBlockingQueue<ProcessCommand>();

    DatapointRepositoryTask(DatapointRepository repository) {
        this.repository = repository;
    }

    public BlockingQueue<ProcessCommand> getMeasurementsQueue() {
        return measurementsQueue;
    }

    @Override
    public void run() {
        try {
            for (; ; ) {
                ProcessCommand command = measurementsQueue.take();
                flush(command.getMeasurements(), command.getTimeMs());
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
            calculator = new Aggregator(repository.getRollupPeriodMs());
            aggregators.put(key, calculator);
        }

        calculator.publish(datapoint, timeMs);
    }

    private String id(Measurement datapoint) {
        return datapoint.company + "!" + datapoint.cluster + "!" + datapoint.member + "!" + datapoint.id + "!" + datapoint.metricName;
    }

    static class Node {
        Node next;
        long timestampMs;
        double maximum = Double.MIN_VALUE;
        double minimum = Double.MAX_VALUE;
        long count;
        double sum;
    }

    static class Aggregator {

        private Datapoint template;
        private Node head;
        private final long maximumRollupMs;

        Aggregator(long maximumRollupMs) {
            this.maximumRollupMs = maximumRollupMs;
        }

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
}
