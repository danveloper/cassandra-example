package com.hazelcast.webmonitor.repositories;

import com.hazelcast.webmonitor.model.WarningMessage;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.ColumnSlice;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import static me.prettyprint.hector.api.factory.HFactory.createMutator;

public class WarningMessageRepository extends AbstractRepository {

    private final ColumnFamilyDefinition warningMessageColumnFamily;

    public WarningMessageRepository(Cluster cluster, Keyspace keyspace) {
        super(cluster, keyspace);

        warningMessageColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), "WarningMessages", ComparatorType.UTF8TYPE);

        add(warningMessageColumnFamily);
    }

    public void delete(String key) {
        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        mutator.addDeletion(key, warningMessageColumnFamily.getName());
        mutator.execute();
    }

    public WarningMessage get(String clusterName, String date) {
        SliceQuery<String, String, String> q = HFactory.createSliceQuery(keyspace, StringSerializer.get(), StringSerializer.get(), StringSerializer.get());
        q.setColumnFamily(warningMessageColumnFamily.getName())
                .setKey(clusterName+":"+date)
                .setColumnNames("message", "cluster", "date");
        QueryResult<ColumnSlice<String, String>> result = q.execute();
        ColumnSlice<String, String> columnSlice = result.get();
        if (columnSlice.getColumns().isEmpty()) {
            return null;
        }

        return new WarningMessage(
                columnSlice.getColumnByName("date").getValue(),
                columnSlice.getColumnByName("message").getValue(),
                columnSlice.getColumnByName("cluster").getValue()
        );
    }

    public void save(WarningMessage alert) {
        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());

        String rowId = alert.getClusterName() + ":" + alert.getDate();
        mutator.addInsertion(rowId, warningMessageColumnFamily.getName(), HFactory.createStringColumn("message", alert.getMessage()))
                .addInsertion(rowId, warningMessageColumnFamily.getName(), HFactory.createStringColumn("cluster", alert.getClusterName()))
                .addInsertion(rowId, warningMessageColumnFamily.getName(), HFactory.createStringColumn("date", alert.getDate()));
        mutator.execute();
    }

    /*
    public List<String> getMessageKeys() {
        List<String> keys = new ArrayList<String>();
        try {
            FastIterator iterator = warningMessageTree.keys();
            String key = (String) iterator.next();
            while (key != null) {
                keys.add(key);
                key = (String) iterator.next();
            }
        } catch (IOException e) {
            logger.warning(e);
        }
        Comparator comparator = Collections.reverseOrder();
        Collections.sort(keys, comparator);
        return keys;
    } */


}
