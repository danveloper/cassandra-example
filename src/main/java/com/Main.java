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
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {
    private final static Logger logger = Logger.getLogger(RollupRunnable.class);

    private final Cluster cluster;
    private final Keyspace keyspace;
    private final CustomersRepository customersRepository;
    private final DatapointRepository rawRepo;
    private final DatapointRepository avgSecondRepo;
    private final DatapointRepository avgFiveSecondRepo;
    private final DatapointRepository avg30SecondRepo;
    private final DatapointRepository maxFiveSecondRepo;
    private final RollupScheduler scheduler;
    private final RollupSchedulerRepository schedulerRepository;
    private final BlockingQueue<Measurement> measurementQueue = new LinkedBlockingQueue<Measurement>();

    public Main() {
        cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
        keyspace = createKeyspace(cluster, "Measurements");

        customersRepository = new CustomersRepository(cluster, keyspace);
        schedulerRepository = new RollupSchedulerRepository(cluster, keyspace);
        rawRepo = new DatapointRepository(cluster, keyspace, "Measurements", (int) TimeUnit.HOURS.toMillis(1));
        avgSecondRepo = new DatapointRepository(cluster, keyspace, "AverageSecond", (int) TimeUnit.HOURS.toMillis(1));
        avgFiveSecondRepo = new DatapointRepository(cluster, keyspace, "AverageFiveSeconds", (int) TimeUnit.HOURS.toMillis(1));
        avg30SecondRepo = new DatapointRepository(cluster, keyspace, "Average30Seconds", (int) TimeUnit.HOURS.toMillis(1));
        maxFiveSecondRepo = new DatapointRepository(cluster, keyspace, "MaxFiveSeconds", (int) TimeUnit.HOURS.toMillis(1));

        scheduler = new RollupScheduler(schedulerRepository, customersRepository);
        scheduler.schedule(rawRepo, avgSecondRepo, new AggregateFunctionFactory(AverageFunction.class), 1000);
        scheduler.schedule(avgSecondRepo, avgFiveSecondRepo, new AggregateFunctionFactory(AverageFunction.class), 5000);
        scheduler.schedule(avgFiveSecondRepo, avg30SecondRepo, new AggregateFunctionFactory(AverageFunction.class), 30000);
        scheduler.schedule(avgSecondRepo, maxFiveSecondRepo, new AggregateFunctionFactory(MaximumFunction.class), 5000);
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
        System.out.println("Customers: " + customersRepository.getCustomers());

        new GenerateMeasurementsThread(measurementQueue, customer).start();

        new Thread() {
            public void run() {
                try {
                    for (; ; ) {
                        Measurement measurement = measurementQueue.take();
                        for (Map.Entry<String, String> entry : measurement.map.entrySet()) {
                            String sensor = measurement.environment + ":" + measurement.machine + ":" + measurement.subject + ":" + entry.getKey();
                            rawRepo.insert(measurement.customer, sensor, measurement.timeMs, entry.getValue());
                        }
                    }
                } catch (Throwable t) {
                    logger.error(t);
                }
            }
        }.start();

        Thread.sleep(10000);

        long endTime = System.currentTimeMillis();

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
