package com.hazelcast.webmonitor.cassandra.repositories;

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

public class CompanyRepository extends AbstractRepository {

    private final ColumnFamilyDefinition customerColumnFamily;

    public CompanyRepository(Cluster cluster, Keyspace keyspace) {
        super(cluster, keyspace);

        customerColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), "company", ComparatorType.UTF8TYPE);

        add(customerColumnFamily);
    }

    public void save(String companyName) {
        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        HColumn<String, String> customerColumn = HFactory.createColumn(
                companyName,
                companyName,
                StringSerializer.get(),
                StringSerializer.get());
        mutator.addInsertion("foo", customerColumnFamily.getName(), customerColumn);
        mutator.execute();
    }

    public Set<String> getCompanyNames() {
        SliceQuery<String, String, String> query = createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get())
                .setKey("foo")
                .setColumnFamily(customerColumnFamily.getName());

        String begin = Character.toString(Character.MIN_VALUE);
        String end = Character.toString(Character.MAX_VALUE);

        ColumnSliceIterator<String, String, String> iterator =
                new ColumnSliceIterator<String, String, String>(query, begin, end, false);

        HashSet<String> result = new HashSet<String>();
        while (iterator.hasNext()) {
            HColumn<String, String> c = iterator.next();
            result.add(c.getName());
        }
        return result;
    }
}
