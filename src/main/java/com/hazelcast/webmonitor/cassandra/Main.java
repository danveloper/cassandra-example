package com.hazelcast.webmonitor.cassandra;

import com.hazelcast.webmonitor.cassandra.datapoints.*;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.webmonitor.cassandra.datapoints.DatapointUtils.matchingTimestamps;
import static com.hazelcast.webmonitor.cassandra.datapoints.DatapointUtils.print;

public class Main {


    public static void main(String[] args) throws Exception {
        Cluster cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
        Keyspace keyspace = createKeyspace(cluster, "Measurements");
        Executor executor = new ThreadPoolExecutor(10, 10, 0, TimeUnit.SECONDS, new LinkedBlockingQueue<Runnable>());
        MeasurementCollector collector = new MeasurementCollector(cluster, keyspace, new int[]{1, 5, 10, 30, 60}, 8, executor);
        collector.start();

        long startTimeMs = System.currentTimeMillis();

        generateMeasurements(collector);

        long endTimeMs = System.currentTimeMillis();

        DatapointQuery query = new DatapointQuery();
        query.company = "hazelcast";
        query.cluster = "dev";
        query.id = "map1";
        query.metric = Metrics.IMAP_READ_LATENCY;
        query.maxResult = Integer.MAX_VALUE;
        query.beginMs = startTimeMs;
        query.endMs = endTimeMs;

        List<Datapoint> readLatency = collector.getRepository(1).slice(query);

        DatapointQuery readCountQuery = new DatapointQuery(query);
        readCountQuery.metric = Metrics.IMAP_READ_COUNT;
        List<Datapoint> readCount = collector.getRepository(1).slice(readCountQuery);

        print(readLatencyPerRead(readCount, readLatency, 1));

        //DatapointQuery latencyQuery = new DatapointQuery(query);
        //latencyQuery.metric = IMAP_READ_COUNT;

        //System.out.println("Per 10 seconds");
        //print(collector.getRepository(10).slice(latencyQuery));

        System.exit(0);
    }

    public static List<Datapoint> readLatencyPerRead(List<Datapoint> readCounts, List<Datapoint> readLatencies, int timeSeconds) {
        List<Pair> matching = matchingTimestamps(readCounts, readLatencies);

        List<Datapoint> results = new LinkedList<Datapoint>();
        for (Pair pair: matching) {
            Datapoint readCount = pair.getLeft();
            Datapoint readLatency= pair.getRight();

            Datapoint result = new Datapoint(readLatency);
            double reads = readCount.velocity * timeSeconds;
            double latency = readLatency.velocity * timeSeconds;
            result.metricName = Metrics.IMAP_LATENCY_PER_READ;
            result.maximum = 0;
            result.minimum = 0;
            result.velocity = 0;
            result.average = latency / reads;
            results.add(result);
        }

        return results;
    }

    private static void generateMeasurements(MeasurementCollector collector) throws InterruptedException {
        long totalLatency = 0;
        long totalReadCount = 0;
        for (int k = 0; k < 10; k++) {
            Thread.sleep(1000);

            Measurement readCount = new Measurement();
            readCount.metricName = Metrics.IMAP_READ_COUNT;
            readCount.timestampMs = System.currentTimeMillis();
            readCount.cluster = "dev";

            totalReadCount += 1;//200 + Math.round(200 * Math.sin(k / 100.0));
            readCount.value = totalReadCount;
            readCount.member = "192.168.1.1";
            readCount.id = "map1";
            readCount.company = "hazelcast";

            collector.publish(readCount);

            totalLatency += 50;//500 + 200 * Math.sin(k / 100.0);

            Measurement readLatency = new Measurement(readCount);
            readLatency.metricName = Metrics.IMAP_READ_LATENCY;
            readLatency.value = totalLatency;

            collector.publish(readLatency);

            System.out.println("Published " + k);
        }
    }

    private static Keyspace createKeyspace(Cluster cluster, String keyspaceName) {
        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspaceName);
        if (keyspaceDef != null) {
            cluster.dropKeyspace(keyspaceName);
        }

        keyspaceDef = HFactory.createKeyspaceDefinition(
                keyspaceName,
                ThriftKsDef.DEF_STRATEGY_CLASS,
                1,
                new LinkedList<ColumnFamilyDefinition>());

        cluster.addKeyspace(keyspaceDef, true);

        return HFactory.createKeyspace(keyspaceDef.getName(), cluster);
    }
}
