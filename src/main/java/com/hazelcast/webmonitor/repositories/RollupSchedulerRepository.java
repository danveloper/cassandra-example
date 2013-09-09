package com.hazelcast.webmonitor.repositories;

import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.HashSet;
import java.util.Set;

import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;

public class RollupSchedulerRepository extends AbstractRepository {

    private final ColumnFamilyDefinition schedulerColumnFamily;

    public RollupSchedulerRepository(Cluster cluster, Keyspace keyspace) {
        super(cluster, keyspace);

        schedulerColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), "RollupSchedulerRepository", ComparatorType.UTF8TYPE);

        add(schedulerColumnFamily);
    }

    public void save(String customer) {
        //inserts the sensor value
        Mutator<String> customerMutator = createMutator(keyspace, StringSerializer.get());
        HColumn<String, String> customerColumn = HFactory.createColumn(
                customer,
                customer,
                StringSerializer.get(),
                StringSerializer.get());
        customerMutator.addInsertion("foo", schedulerColumnFamily.getName(), customerColumn);
        customerMutator.execute();
    }

    public Set<String> getCustomers() {
        SliceQuery<String, String, String> query = createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                .setKey("foo")
                .setColumnFamily(schedulerColumnFamily.getName());
        String endString = Character.toString(Character.MAX_VALUE);

        ColumnSliceIterator<String, String, String> iterator =
                new ColumnSliceIterator<String, String, String>(query, "a", endString, false);

        HashSet<String> result = new HashSet<String>();
        while (iterator.hasNext()) {
            HColumn<String, String> c = iterator.next();
            result.add(c.getName());
        }
        return result;
    }
}
