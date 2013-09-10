package com.hazelcast.webmonitor.newdatapoint;

import com.hazelcast.webmonitor.repositories.AbstractRepository;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;

public class NewDatapointRepository extends AbstractRepository {

    private final ColumnFamilyDefinition datapointColumnFamily;

    public NewDatapointRepository(Cluster cluster, Keyspace keyspace) {
        super(cluster, keyspace);

        datapointColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), "NewDatapoint");
        //Validator to use for keys
        datapointColumnFamily.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
        //Defines how to store, compare and validate the column names
        datapointColumnFamily.setComparatorType(ComparatorType.TIMEUUIDTYPE);
        //Validator to use for values in columns
        datapointColumnFamily.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());

        add(datapointColumnFamily);
    }

    public void insert(Datapoint datapoint) {
        //if the metric.name would be used as row-key;
    }
}
