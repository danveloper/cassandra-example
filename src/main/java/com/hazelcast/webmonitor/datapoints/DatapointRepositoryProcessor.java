package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.Measurement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

class DatapointRepositoryProcessor {
    private final DatapointRepository repository;
    private final Map<String, Aggregator> aggregators = new HashMap<String, Aggregator>();
    private final int rollupPeriodMs;
    private final int historyLength;
    private final int timeMod;

    DatapointRepositoryProcessor(DatapointRepository repository) {
        this.repository = repository;
        this.rollupPeriodMs = repository.getRollupPeriodMs();

        if (rollupPeriodMs <= 10000) {
            timeMod = 1000;
        } else {
            timeMod = rollupPeriodMs / 10000;
            if(rollupPeriodMs%10000!=0){
                throw new IllegalArgumentException("Illegal rollup period: "+rollupPeriodMs);
            }
        }

        this.historyLength = Math.min(10, (rollupPeriodMs / 1000) + 1);
    }

    int getHistoryLength() {
        return historyLength;
    }

    int rollupPeriodMs() {
        return rollupPeriodMs;
    }

    public void process(ProcessCommand command) {
        long timeMs = timeMod * (command.getTimeMs() / timeMod);
        flush(command.getMeasurements(), timeMs);
    }

    private void flush(List<Measurement> measurements, long timeMs) {
        if (measurements.isEmpty()) {
            return;
        }

        for (Measurement datapoint : measurements) {
            //Datapoint memberAgnosticDatapoint = new Datapoint(datapoint);
            //memberAgnosticDatapoint.member="";

            //Datapoint idAgnosticDatapoint = new Datapoint(datapoint);
            //idAgnosticDatapoint.id="";

            publish(datapoint, timeMs);
            //publish(memberAgnosticDatapoint);
            //publish(idAgnosticDatapoint);
        }

        for (Aggregator aggregator : aggregators.values()) {
            aggregator.aggregate(timeMs);
        }
    }

    private void publish(Measurement datapoint, long timeMs) {
        String key = id(datapoint);

        Aggregator calculator = aggregators.get(key);
        if (calculator == null) {
            calculator = new Aggregator();
            aggregators.put(key, calculator);
        }

        calculator.publish(datapoint, timeMs);
    }

    private String id(Measurement datapoint) {
        return datapoint.company + "!" + datapoint.cluster + "!" + datapoint.member + "!" + datapoint.id + "!" + datapoint.metricName;
    }

    private static class Node {
        Node next;
        long timestampMs;
        double maximum = Double.MIN_VALUE;
        double minimum = Double.MAX_VALUE;
        long count;
        double sum;
    }

    private class Aggregator {

        private Datapoint template;
        private Node head;

        private void publish(Measurement datapoint, long timeMs) {
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

        private void aggregate(long timeMs) {
            double maxvalue = Long.MIN_VALUE;
            double minvalue = Long.MAX_VALUE;
            int items = 0;
            double sum = 0;

            Node node = head;
            Node previous = null;
            Node tail = null;

            int k = 0;
            while (node != null) {
                if (k == historyLength + 1) {
                    if (previous == null) {
                        head = null;
                    } else {
                        previous.next = null;
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
