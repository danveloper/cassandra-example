package com.hazelcast.webmonitor.repositories;

import com.eaio.uuid.UUID;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.exceptions.HInvalidRequestException;

import java.util.List;

public class AbstractRepository {

    protected final Keyspace keyspace;
    protected final Cluster cluster;

    public AbstractRepository(Cluster cluster,Keyspace keyspace) {
        this.keyspace = keyspace;
        this.cluster = cluster;
    }

    public static UUID toTimeUUID(long time) {
        return new UUID(TimeUUIDUtils.getTimeUUID(time).toString());
    }

    public void add(ColumnFamilyDefinition columnFamilyDefinition){
        if(contains(columnFamilyDefinition)){
            return;
        }

        try{
             cluster.addColumnFamily(columnFamilyDefinition);
        }  catch (HInvalidRequestException e){
            if(e.getMessage().toLowerCase().contains("already existing"))
                return;

            throw e;
        }
    }

    private boolean contains(ColumnFamilyDefinition columnFamilyDefinition) {
        List<ColumnFamilyDefinition> columnFamilyDefinitions = cluster.describeKeyspace(keyspace.getKeyspaceName()).getCfDefs();

        for (ColumnFamilyDefinition def : columnFamilyDefinitions) {
            if (def.getName().equals(columnFamilyDefinition.getName())) {
                return true;
            }
        }

        return false;
    }
}
