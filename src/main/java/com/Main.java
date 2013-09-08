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
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

    private final Cluster cluster;
    private final Keyspace keyspace;
    private final CustomersRepository customersRepository;
    private final DatapointRepository rawRepo;
    private final DatapointRepository avgSecondRepo;
    private final DatapointRepository avgFiveSecondRepo;
    private final DatapointRepository avg30SecondRepo;
    private final DatapointRepository maxFiveSecondRepo;
    private final ThreadPoolExecutor executor;
    private final ScheduledThreadPoolExecutor scheduler;

    public Main() {
        cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
        keyspace = createKeyspace(cluster, "Measurements");

        customersRepository = new CustomersRepository(cluster, keyspace);
        rawRepo = new DatapointRepository(cluster, keyspace, "Measurements", (int) TimeUnit.HOURS.toMillis(1));
        avgSecondRepo = new DatapointRepository(cluster, keyspace, "averageSecond", (int) TimeUnit.HOURS.toMillis(1));
        avgFiveSecondRepo = new DatapointRepository(cluster, keyspace, "averageFiveSeconds", (int) TimeUnit.HOURS.toMillis(1));
        avg30SecondRepo = new DatapointRepository(cluster, keyspace, "average30Seconds", (int) TimeUnit.HOURS.toMillis(1));
        maxFiveSecondRepo = new DatapointRepository(cluster, keyspace, "maxFiveSeconds", (int) TimeUnit.HOURS.toMillis(1));
        scheduler = new ScheduledThreadPoolExecutor(10);
        executor = new ThreadPoolExecutor(10, 50, 1, TimeUnit.MINUTES, new LinkedBlockingQueue<Runnable>());

        scheduler.scheduleAtFixedRate(
                new RollupRunnable(
                        rawRepo,
                        avgSecondRepo,
                        "average 1 second",
                        new AggregateFunctionFactory(AverageFunction.class),
                        executor, customersRepository),
                0, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(
                new RollupRunnable(
                        avgSecondRepo,
                        avgFiveSecondRepo,
                        "average 5 seconds",
                        new AggregateFunctionFactory(AverageFunction.class),
                        executor, customersRepository),
                0, 5, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(
                new RollupRunnable(
                        avgFiveSecondRepo,
                        avg30SecondRepo,
                        "average 30 seconds",
                        new AggregateFunctionFactory(AverageFunction.class),
                        executor, customersRepository),
                0, 30, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(
                new RollupRunnable(
                        avgSecondRepo,
                        maxFiveSecondRepo,
                        "maximum 5 seconds",
                        new AggregateFunctionFactory(MaximumFunction.class),
                        executor, customersRepository),
                0, 5, TimeUnit.SECONDS);
    }

    public void registerCustomer(String customer) {
        rawRepo.createColumnFamilies(customer);
        avgSecondRepo.createColumnFamilies(customer);
        avgFiveSecondRepo.createColumnFamilies(customer);
        avg30SecondRepo.createColumnFamilies(customer);
        maxFiveSecondRepo.createColumnFamilies(customer);
        customersRepository.save(customer);
    }

    public void start() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        String customer = "foo";
        registerCustomer(customer);
        System.out.println("Customers: "+customersRepository.getCustomers());

        new GenerateMeasurementsThread(rawRepo,customer, "dev:192.168.1.1:5701").start();
        new GenerateMeasurementsThread(rawRepo, customer, "dev:192.168.1.1:5702").start();
        new GenerateMeasurementsThread(rawRepo, customer, "prod:192.168.1.1:5703").start();
        new GenerateMeasurementsThread(rawRepo, customer, "prod:192.168.1.1:5704").start();

        Thread.sleep(10000);

        long endTime = System.currentTimeMillis();

        System.out.println("sensorname:");
        for (Iterator<String> it = rawRepo.sensorNameIterator(customer, startTime, endTime); it.hasNext(); ) {
            System.out.println("    " + it.next());
        }

        System.out.println("avg 5 seconds:");
        print(avgFiveSecondRepo, customer, startTime, endTime);

        System.out.println("finished");
        System.exit(0);
    }

    public static void main(String[] args) throws InterruptedException {
        Main main = new Main();
        main.start();
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
