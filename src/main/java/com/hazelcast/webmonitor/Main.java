package com.hazelcast.webmonitor;

import com.hazelcast.webmonitor.datapoints.Datapoint;
import com.hazelcast.webmonitor.datapoints.MeasurementCollector;
import com.hazelcast.webmonitor.datapoints.DatapointQuery;
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

public class Main {

    public final static String readCountMeasurementName = "IMap.readCount";
    public final static String totalReadLatencyMeasurementName = "IMap.totalReadLatency";
    public final static String readLatencyMeasurementName = "IMap.readLatency";

    public static void main(String[] args) throws Exception {
        Cluster cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
        Keyspace keyspace = createKeyspace(cluster, "Measurements");
        Executor executor = new ThreadPoolExecutor(10,10,0, TimeUnit.SECONDS,new LinkedBlockingQueue<Runnable>());
        MeasurementCollector collector = new MeasurementCollector(cluster, keyspace, new int[]{1, 5, 10, 30, 60},8,executor);
        collector.start();

        long startTimeMs = System.currentTimeMillis();

        generateMeasurements(collector);

        long endTimeMs = System.currentTimeMillis();

        DatapointQuery query = new DatapointQuery();
        query.company = "hazelcast";
        query.cluster = "dev";
        query.id = "map1";
        query.metric = readLatencyMeasurementName;
        query.maxResult = Integer.MAX_VALUE;
        query.beginMs = startTimeMs;
        query.endMs = endTimeMs;

        //System.out.println("Per 1 seconds");
        //print(collector.getRepository(1).slice(query));

        System.out.println("Per 5 seconds");
        print(collector.getRepository(1).slice(query));

        System.out.println("Per 10 seconds");
        print(collector.getRepository(10).slice(query));

        //DatapointQuery latencyQuery = new DatapointQuery(query);
        //latencyQuery.metric = readCountMeasurementName;

        //System.out.println("Per 10 seconds");
        //print(collector.getRepository(10).slice(latencyQuery));

        System.exit(0);
    }

    private static void generateMeasurements(MeasurementCollector collector) throws InterruptedException {
        long totalLatency = 0;
        long totalReadCount = 0;
        for (int k = 0; k < 600; k++) {
            Thread.sleep(100);

            Measurement readCount = new Measurement();
            readCount.metricName = readCountMeasurementName;
            readCount.timestampMs = System.currentTimeMillis();
            readCount.cluster = "dev";

            totalReadCount+=100;//200 + Math.round(200 * Math.sin(k / 100.0));
            readCount.value =totalReadCount;
            readCount.member = "192.168.1.1";
            readCount.id = "map1";
            readCount.company = "hazelcast";

            collector.publish(readCount);

            totalLatency += 100;//500 + 200 * Math.sin(k / 100.0);

            Measurement readLatency = new Measurement(readCount);
            readLatency.metricName = readLatencyMeasurementName;
            readLatency.value += totalLatency / readCount.value;

            collector.publish(readLatency);

            System.out.println("Published " + k);
        }
    }

    public static void print(List<Datapoint> datapoints) {
        int k = 1;
        for (Datapoint datapoint : datapoints) {
            System.out.println(k + " " + datapoint);
            k++;
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
