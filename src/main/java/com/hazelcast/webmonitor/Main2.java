package com.hazelcast.webmonitor;

import com.hazelcast.webmonitor.newdatapoint.Datapoint;
import com.hazelcast.webmonitor.newdatapoint.DatapointCollector;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import java.util.LinkedList;
import java.util.List;

public class Main2 {

    public static void main(String[] args) throws Exception {
        Cluster cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
        Keyspace keyspace = createKeyspace(cluster, "Measurements");
        DatapointCollector collector = new DatapointCollector(cluster, keyspace, new int[]{1,5,10,30});
        collector.start();

        String metricName = "IMap.readCount";

        long startTimeMs = System.currentTimeMillis();
        for (int k = 0; k < 3000; k++) {
            Thread.sleep(100);

            Datapoint datapoint = new Datapoint();
            datapoint.metricName = "IMap.readCount";
            datapoint.timestampMs = System.currentTimeMillis();
            datapoint.cluster = "dev";
            if (k % 2 == 1) {
                datapoint.value = Math.round(500 + 200 * Math.sin(k / 100.0));
                datapoint.member = "192.168.1.1";
            } else {
                datapoint.value = Math.round(500 + 200 * Math.cos(k / 100.0));
                datapoint.member = "192.168.1.2";
            }
            datapoint.id = "map1";
            datapoint.company = "hazelcast";

            collector.publish(datapoint);
            System.out.println("Published " + k);
        }
        long endTimeMs = System.currentTimeMillis();

        System.out.println("max Per 1 seconds");
        print(collector.getRepository(1).slice("max(IMap.readCount)", startTimeMs, endTimeMs));

//        System.out.println("max Per 5 seconds");
//        print(collector.getRepository(5).slice("max(IMap.readCount)", startTimeMs, endTimeMs));

        //System.out.println("max Per 10 seconds");
        //print(collector.getRepository(10).slice("max(IMap.readCount)", startTimeMs, endTimeMs));

        //System.out.println("min Per 10 seconds");
        //print(collector.getRepository(10).slice("min(IMap.readCount)", startTimeMs, endTimeMs));

        System.out.println("avg Per 10 seconds");
        print(collector.getRepository(30).slice("avg(IMap.readCount)", startTimeMs, endTimeMs));

        //System.out.println("max Per 30 seconds");
        //print(collector.getRepository(30).slice("max(IMap.readCount)", startTimeMs, endTimeMs));

        System.exit(0);
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
