package com.hazelcast.webmonitor;

import com.eaio.uuid.UUID;
import com.hazelcast.webmonitor.repositories.ActiveMembersRepository;
import com.hazelcast.webmonitor.repositories.CompanyRepository;
import com.hazelcast.webmonitor.repositories.DatapointRepository;
import com.hazelcast.webmonitor.repositories.RollupSchedulerRepository;
import com.hazelcast.webmonitor.scheduler.*;
import me.prettyprint.cassandra.service.ThriftKsDef;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.log4j.Logger;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class Main {
    private final static Logger logger = Logger.getLogger(RollupRunnable.class);

    private final Cluster cluster;
    private final Keyspace keyspace;
    private final CompanyRepository companyRepository;
    private final DatapointRepository rawRepo;
    private final DatapointRepository avgSecondRepo;
    private final DatapointRepository avgFiveSecondRepo;
    private final DatapointRepository avg30SecondRepo;
    private final RollupScheduler scheduler;
    private final RollupSchedulerRepository schedulerRepository;
    private final BlockingQueue<Measurements> measurementQueue = new LinkedBlockingQueue<Measurements>();
    private final DatapointRepository compactedSecondRepo;
    private final DatapointRepository velocity1SecondRepo;
    private final DatapointRepository velocity5SecondRepo;
    private final ActiveMembersRepository activeMembersRepository;

    public Main() {
        //startCassandra();

        cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
        keyspace = createKeyspace(cluster, "Measurements");

        companyRepository = new CompanyRepository(cluster, keyspace);
        schedulerRepository = new RollupSchedulerRepository(cluster, keyspace);
        activeMembersRepository = new ActiveMembersRepository(cluster,keyspace);

        rawRepo = new DatapointRepository(cluster, keyspace, "Measurements", (int) TimeUnit.HOURS.toMillis(1));

        avgSecondRepo = new DatapointRepository(cluster, keyspace, "AverageSecond", (int) TimeUnit.HOURS.toMillis(1));
        compactedSecondRepo = new DatapointRepository(cluster, keyspace, "Compacted", (int) TimeUnit.HOURS.toMillis(1));

        avgFiveSecondRepo = new DatapointRepository(cluster, keyspace, "AverageFiveSeconds", (int) TimeUnit.HOURS.toMillis(1));
        avg30SecondRepo = new DatapointRepository(cluster, keyspace, "Average30Seconds", (int) TimeUnit.HOURS.toMillis(1));

        scheduler = new RollupScheduler(schedulerRepository, companyRepository);

        velocity1SecondRepo = new DatapointRepository(cluster, keyspace, "Velocity1Second", (int) TimeUnit.HOURS.toMillis(1));
        velocity5SecondRepo = new DatapointRepository(cluster, keyspace, "Velocity5Second", (int) TimeUnit.HOURS.toMillis(1));

        //  scheduler.schedule(rawRepo, avgSecondRepo, AvgRollupRunnable.class, 1000);
        scheduler.schedule("avg 1 second",rawRepo, avgSecondRepo, AvgRollupRunnable.class, 1000);
        scheduler.schedule("compact 1 second",rawRepo, avgSecondRepo, CompactRollupRunnable.class, 1000);
      //  scheduler.schedule(rawRepo2, avgSecondRepo, AvgRollupRunnable.class, 1000);
         scheduler.schedule("avg 5 second",avgSecondRepo, avgFiveSecondRepo, AvgRollupRunnable.class, 5000);
         scheduler.schedule("avg 30 second",avgFiveSecondRepo, avg30SecondRepo, AvgRollupRunnable.class, 30000);

        scheduler.schedule("velocity 1 second",rawRepo, velocity1SecondRepo, VelocityRollupRunnable.class, 1000);
        scheduler.schedule("velocity 5 second",velocity1SecondRepo, velocity5SecondRepo, AvgRollupRunnable.class, 5000);
    }

    public static void startCassandra() {
        System.setProperty("cassandra.config", "file:/java/projects/junk/cassandra-example/cassandra.yaml");
        System.setProperty("cassandra-foreground", "true");

        CassandraDaemon cassandraDaemon = new CassandraDaemon();
        cassandraDaemon.activate();
    }

    public void registerCustomer(String company) {
        rawRepo.createColumnFamilies(company);
        avgSecondRepo.createColumnFamilies(company);
        avgFiveSecondRepo.createColumnFamilies(company);
        avg30SecondRepo.createColumnFamilies(company);
        compactedSecondRepo.createColumnFamilies(company);

        velocity1SecondRepo.createColumnFamilies(company);
        velocity5SecondRepo.createColumnFamilies(company);

        companyRepository.save(company);
    }

    public void start() throws InterruptedException {
        long startTime = System.currentTimeMillis();

        String company = "hazelcast";
        registerCustomer(company);

        new GenerateMeasurementsThread(measurementQueue, company).start();

        new Thread() {
            public void run() {
                try {
                    for (; ; ) {
                        Measurements measurement = measurementQueue.take();
                        for (Map.Entry<String, Long> entry : measurement.map.entrySet()) {

                            String sensor = measurement.environment + "!" + measurement.machine + "!" + measurement.subject + "!" + entry.getKey();
                            rawRepo.insert(measurement.customer, sensor, measurement.timestampMs, entry.getValue());

                            activeMembersRepository.insert(measurement.customer,measurement.environment, measurement.machine,measurement.timestampMs);
                        }
                    }
                } catch (Throwable t) {
                    logger.error(t);
                }
            }
        }.start();

       for(int k=0;k<20;k++){
           long time = System.currentTimeMillis();
           Set<String> members = activeMembersRepository.getActiveMembers(company,"dev",time-1000,time+1000);
           System.out.println(members);
           Thread.sleep(1000);
       }

        //Thread.sleep(20000);

        long endTime = System.currentTimeMillis();

        print("velocity 1 seconds",velocity1SecondRepo, company, startTime, endTime);
        print("velocity 5 seconds",velocity5SecondRepo, company, startTime, endTime);

        System.out.println("finished");
        System.exit(0);
    }


    public static void main(String[] args) throws InterruptedException {
        Main main = new Main();
        main.start();
    }

    private static void print(String desc, DatapointRepository repository, String customer, long startTime, long endTime) {
        System.out.println(desc);


        for (Iterator<String> nameIt = repository.sensorNameIterator(customer, startTime, endTime); nameIt.hasNext(); ) {
            String name = nameIt.next();
            System.out.println(name);
            int k = 1;
            for (Map.Entry<UUID, Long> result : repository.selectColumnsBetween(customer, name, startTime, endTime).entrySet()) {
                UUID time = result.getKey();
                System.out.println(k + "         time:" + time.getTime() + " value:" + result.getValue());
                k++;
            }
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
