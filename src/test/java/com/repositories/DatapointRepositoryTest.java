package com.repositories;

import com.repositories.DatapointRepository;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static me.prettyprint.hector.api.factory.HFactory.createKeyspaceDefinition;
import static org.junit.Assert.*;

public class DatapointRepositoryTest {

    public  final String KEYSPACE = generateKeySpaceName();
    String customer = "hazelcast";

    private String generateKeySpaceName() {
        return "Test_"+ UUID.randomUUID().toString().replace("-","");
    }

    private Cluster cluster;
    private DatapointRepository repository;

    @Before
    public void setUp() {
        cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
        Keyspace keyspace = createKeyspace(cluster, KEYSPACE);
        repository = new DatapointRepository(cluster, keyspace, "peter",(int) TimeUnit.HOURS.toMillis(1));
        repository.createColumnFamilies(customer);
    }

    @After
    public void tearDown(){
       if(cluster!=null){
           KeyspaceDefinition keyspaceDef = cluster.describeKeyspace(KEYSPACE);
           if (keyspaceDef != null) {
               cluster.dropKeyspace(KEYSPACE);
           }
       }
    }

    private static Keyspace createKeyspace(Cluster cluster, String keyspaceName) {
        KeyspaceDefinition keyspaceDef = createKeyspaceDefinition(keyspaceName);
        cluster.addKeyspace(keyspaceDef, true);
        return HFactory.createKeyspace(keyspaceDef.getName(), cluster);
    }


    @Test
    public void increment() {
        long time = System.currentTimeMillis();
        repository.insert(customer, "foo", time, 1);
        repository.insert(customer, "foo", time, 2);
        Long result = repository.read(customer,"foo", time);
        assertEquals(new Long(3), result);
    }


    @Test
    public void readExisting() {
        long time = System.currentTimeMillis();
        repository.insert(customer, "foo", time, 1);
        Long result = repository.read(customer,"foo", time);
        assertEquals(new Long(1), result);
    }

    @Test
    public void readNonExisting() {
        long time = System.currentTimeMillis();
        Long result = repository.read(customer,"foo", time);
        assertNull(result);
    }

    @Test
    public void sensorNameIterator_noNames() {
        long time = System.currentTimeMillis();
        Iterator it = repository.sensorNameIterator(customer,time, time);
        assertFalse(it.hasNext());
    }

    @Test
    public void sensorNameIterator_multipleNames() {
        long time = System.currentTimeMillis();
        repository.insert(customer, "foo", time, 1);
        repository.insert(customer, "bar", time, 1);

        Set <String> names = toSet(repository.sensorNameIterator(customer,time, time));
        assertEquals(2, names.size());
        assertTrue(names.contains("foo"));
        assertTrue(names.contains("bar"));
    }

    public Set<String> toSet(Iterator<String> it) {
        HashSet<String> result = new HashSet<String>();
        for (; it.hasNext(); ) {
            result.add(it.next());
        }

        return result;
    }
}
