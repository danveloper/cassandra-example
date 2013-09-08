package com;

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
import java.util.List;
import java.util.Set;

import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;

public class CustomersRepository {

    private final ColumnFamilyDefinition customerColumnFamily;
    private final Cluster cluster;
    private final Keyspace keyspace;

    public CustomersRepository(Cluster cluster, Keyspace keyspace) {
        this.cluster = cluster;
        this.keyspace = keyspace;

        customerColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), "Customer", ComparatorType.UTF8TYPE);

        if(!contains(customerColumnFamily)){
            cluster.addColumnFamily(customerColumnFamily);
        }
    }

    public void save(String customer) {
        //inserts the sensor value
        Mutator <String> customerMutator = createMutator(keyspace, StringSerializer.get());
        HColumn<String, String> customerColumn = HFactory.createColumn(
                customer,
                customer,
                StringSerializer.get(),
                StringSerializer.get());
        customerMutator.addInsertion("foo", customerColumnFamily.getName(), customerColumn);
        customerMutator.execute();
    }

    public Set<String> getCustomers() {
        SliceQuery<String, String, String> query = createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                .setKey("foo")
                .setColumnFamily(customerColumnFamily.getName());
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
