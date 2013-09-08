package com;

import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.KeyspaceDefinition;
import me.prettyprint.hector.api.factory.HFactory;
import org.junit.After;
import org.junit.Before;

import java.util.UUID;

import static me.prettyprint.hector.api.factory.HFactory.createKeyspaceDefinition;

public class AbstractRepositoryTest {

    public final String KEYSPACE = generateKeySpaceName();

    private String generateKeySpaceName() {
        return "Test_" + UUID.randomUUID().toString().replace("-", "");
    }

    public Cluster cluster;
    public Keyspace keyspace;

     public void setUp() {
        cluster = HFactory.getOrCreateCluster("test-cluster", "localhost:9160");
        keyspace = createKeyspace(cluster, KEYSPACE);
    }

    @After
    public void tearDown() {
        if (cluster != null) {
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
}
