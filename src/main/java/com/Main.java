package com;

import com.aggregatefunctions.AggregateFunctionFactory;
import com.aggregatefunctions.AverageFunction;
import com.aggregatefunctions.MaximumFunction;
import com.eaio.uuid.UUID;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.*;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        Cluster cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
        Keyspace keyspace = createKeyspace(cluster, "Measurements");

        String customer = "hazelcast";

        DatapointRepository rawRepo = new DatapointRepository(cluster, keyspace, "Measurements", (int) TimeUnit.HOURS.toMillis(1));
        DatapointRepository avgSecondRepo = new DatapointRepository(cluster, keyspace, "averageSecond", (int) TimeUnit.HOURS.toMillis(1));
        DatapointRepository avgFiveSecondRepo = new DatapointRepository(cluster, keyspace, "averageFiveSeconds", (int) TimeUnit.HOURS.toMillis(1));
        DatapointRepository avg30SecondRepo = new DatapointRepository(cluster, keyspace, "average30Seconds", (int) TimeUnit.HOURS.toMillis(1));
        DatapointRepository maxFiveSecondRepo = new DatapointRepository(cluster, keyspace, "maxFiveSeconds", (int) TimeUnit.HOURS.toMillis(1));


        rawRepo.createColumnFamilies(customer);
        avgSecondRepo.createColumnFamilies(customer);
        avgFiveSecondRepo.createColumnFamilies(customer);
        avg30SecondRepo.createColumnFamilies(customer);
        maxFiveSecondRepo.createColumnFamilies(customer);

        ScheduledExecutorService scheduler = new ScheduledThreadPoolExecutor(10);
        ExecutorService executor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());

        scheduler.scheduleAtFixedRate(
                new RollupRunnable(
                        rawRepo,
                        avgSecondRepo,
                        "average 1 second",
                        new AggregateFunctionFactory(AverageFunction.class),
                        executor, customer),
                0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(
                new RollupRunnable(
                        avgSecondRepo,
                        avgFiveSecondRepo,
                        "average 5 seconds",
                        new AggregateFunctionFactory(AverageFunction.class),
                        executor, customer),
                0, 5, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(
                new RollupRunnable(
                        avgFiveSecondRepo,
                        avg30SecondRepo,
                        "average 30 seconds",
                        new AggregateFunctionFactory(AverageFunction.class),
                        executor, customer),
                0, 30, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(
                new RollupRunnable(
                        avgSecondRepo,
                        maxFiveSecondRepo,
                        "maximum 5 seconds",
                        new AggregateFunctionFactory(MaximumFunction.class),
                        executor, customer),
                0, 5, TimeUnit.SECONDS);

        long startTime = System.currentTimeMillis();

        new GenerateMeasurementsThread(rawRepo, customer, "dev:192.168.1.1:5701").start();
        new GenerateMeasurementsThread(rawRepo, customer, "dev:192.168.1.1:5702").start();
        new GenerateMeasurementsThread(rawRepo, customer, "prod:192.168.1.1:5703").start();
        new GenerateMeasurementsThread(rawRepo, customer, "prod:192.168.1.1:5704").start();

        Thread.sleep(60000);

        long endTime = System.currentTimeMillis();

        System.out.println("sensorname:");
        for (Iterator<String> it = rawRepo.sensorNameIterator(customer, startTime, endTime); it.hasNext(); ) {
            System.out.println("    " + it.next());
        }

  //      System.out.println("average seconds:");
  //      print(avgSecondRepo, customer, startTime, endTime);
 //       System.out.println("average 5 seconds:");
 //       print(avgFiveSecondRepo, customer, startTime, endTime);
 //       System.out.println("max 5 seconds:");
 //       print(maxFiveSecondRepo, customer, startTime, endTime);
        System.out.println("avg 30 seconds:");
        print(avg30SecondRepo, customer, startTime, endTime);

        System.out.println("finished");
        System.exit(0);
    }

    private static void print(DatapointRepository repository, String customer, long startTime, long endTime) {
        for (Iterator<String> nameIt = repository.sensorNameIterator(customer, startTime, endTime); nameIt.hasNext(); ) {
            String name = nameIt.next();
            System.out.println(name);
            int k = 1;
            for (Map.Entry<UUID, String> result : repository.selectColumnsBetween(customer, name, startTime, endTime).entrySet()) {
                UUID time = result.getKey();
                System.out.println(k + "         time:" + time.getTime() + " value:" + result.getValue());
                k++;
            }
        }
    }

    private static Keyspace createKeyspace(Cluster cluster, String keyspaceName) {
        KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(keyspaceName);
        if (keyspaceDef == null) {
            keyspaceDef = HFactory.createKeyspaceDefinition(
                    keyspaceName,
                    ThriftKsDef.DEF_STRATEGY_CLASS,
                    1,
                    new LinkedList<ColumnFamilyDefinition>());

            cluster.addKeyspace(keyspaceDef, true);
        }

        return HFactory.createKeyspace(keyspaceDef.getName(), cluster);
    }
}
