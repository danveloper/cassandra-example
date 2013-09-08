package com;

import com.eaio.uuid.UUID;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
import me.prettyprint.cassandra.service.template.ColumnFamilyResult;
import me.prettyprint.cassandra.service.template.ThriftColumnFamilyTemplate;
import me.prettyprint.cassandra.utils.TimeUUIDUtils;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.ColumnQuery;
import me.prettyprint.hector.api.query.QueryResult;
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.*;

import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;


public class DatapointRepository {
    private final Keyspace keyspace;
    private final Cluster cluster;

    private final ColumnFamilyDefinition datapointColumnFamily;
    private final ThriftColumnFamilyTemplate<String, UUID> datapointTemplate;

    private final ColumnFamilyDefinition sensornameColumnFamily;
    private final int ttlMs;

    public DatapointRepository(Cluster cluster, Keyspace Keyspace, String name, int ttlMs) {
        name = name.toLowerCase();
        this.cluster = cluster;
        this.keyspace = Keyspace;
        this.ttlMs = ttlMs;

        // ================= datapoints ================================

        this.datapointColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), name, ComparatorType.UUIDTYPE);

        addIfNotExist(cluster, keyspace, datapointColumnFamily);

        this.datapointTemplate = new ThriftColumnFamilyTemplate<String, UUID>(
                Keyspace,
                datapointColumnFamily.getName(),
                StringSerializer.get(),
                TimeUUIDSerializer.get());

        // =================== sensor names =============================

        sensornameColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), name + "_names", ComparatorType.COMPOSITETYPE);
        sensornameColumnFamily.setComparatorTypeAlias("(TimeUUIDType, UTF8Type)");
        sensornameColumnFamily.setKeyValidationClass("UTF8Type");
        sensornameColumnFamily.setDefaultValidationClass("UTF8Type");

        addIfNotExist(cluster, keyspace, sensornameColumnFamily);
    }

    private void addIfNotExist(Cluster cluster, Keyspace keyspace, ColumnFamilyDefinition def) {
        //todo:racy
        if (!contains(def, keyspace.getKeyspaceName())) {
            cluster.addColumnFamily(def);
        }
    }

    public static UUID toTimeUUID(long time) {
        return new UUID(TimeUUIDUtils.getTimeUUID(time).toString());
    }

    public void update(String sensor, long timeMs, String value) {
        UUID timeUUID = toTimeUUID(timeMs);

        //inserts the sensor value
        Mutator<String> datapointMutator = createMutator(keyspace, StringSerializer.get());
        HColumn<UUID, String> datapointColumn = HFactory.createColumn(
                timeUUID,
                value,
                TimeUUIDSerializer.get(),
                StringSerializer.get());
        datapointColumn.setTtl(ttlMs);
        datapointMutator.addInsertion(sensor, datapointColumnFamily.getName(), datapointColumn);
        datapointMutator.execute();

        //inserts the sensor name
        Composite timenameColumnKey = new Composite();
        timenameColumnKey.addComponent(timeUUID, TimeUUIDSerializer.get());
        timenameColumnKey.addComponent(sensor, StringSerializer.get());

        Mutator<String> sensornameMutator = createMutator(keyspace, StringSerializer.get());
        HColumn<Composite, String> sensornameColumn = HFactory.createColumn(
                timenameColumnKey,
                sensor,
                new CompositeSerializer(),
                StringSerializer.get());
        sensornameColumn.setTtl(ttlMs);
        sensornameMutator.addInsertion("name", sensornameColumnFamily.getName(), sensornameColumn);
        sensornameMutator.execute();
    }

    public String read(String sensor, long time) {
       // QueryResult<String,UUID> query = HFactory.cr().execute();


        ColumnFamilyResult<String, UUID> res = datapointTemplate.queryColumns(sensor);
        return res.getString(toTimeUUID(time));
    }

    public void delete(String sensor, long time) {
        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        mutator.delete(sensor, datapointColumnFamily.getName(), toTimeUUID(time), TimeUUIDSerializer.get());
        mutator.execute();

        //todo: the sensor name should be deleted.
    }

    public void deleteSensor(String sensor) {
        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        mutator.addDeletion(sensor, datapointColumnFamily.getName());
        mutator.execute();
    }

    public Map<UUID, String> selectColumnsBetween(String sensor, long startMs, long endMs) {
        SliceQuery<String, UUID, String> query = createSliceQuery(keyspace, StringSerializer.get(), TimeUUIDSerializer.get(), StringSerializer.get())
                .setKey(sensor)
                .setColumnFamily(datapointColumnFamily.getName());

        ColumnSliceIterator<String, UUID, String> iterator =
                new ColumnSliceIterator<String, UUID, String>(query, toTimeUUID(startMs), toTimeUUID(endMs), false);

        LinkedHashMap<UUID, String> result = new LinkedHashMap<UUID, String>();
        while (iterator.hasNext()) {
            HColumn<UUID, String> c = iterator.next();
            result.put(c.getName(), c.getValue());
        }
        return result;
    }

    public ColumnSliceIterator<String, UUID, String> dataPointIterator(String sensor, long startMs, long endMs) {
        SliceQuery<String, UUID, String> query = createSliceQuery(keyspace, StringSerializer.get(), TimeUUIDSerializer.get(), StringSerializer.get())
                .setKey(sensor)
                .setColumnFamily(datapointColumnFamily.getName());

        return new ColumnSliceIterator<String, UUID, String>(query, toTimeUUID(startMs), toTimeUUID(endMs), false);
    }

    public Iterator<String> sensorNameIterator(long startMs, long endMs) {
        SliceQuery<String, Composite, String> query = createSliceQuery(keyspace, StringSerializer.get(),
                CompositeSerializer.get(),
                StringSerializer.get());
        query.setColumnFamily(sensornameColumnFamily.getName());
        query.setKey("name");

        // Create a composite search range
        Composite start = new Composite();
        start.addComponent(toTimeUUID(startMs), TimeUUIDSerializer.get());
        start.addComponent("a", StringSerializer.get());

        Composite finish = new Composite();
        finish.addComponent(toTimeUUID(endMs), TimeUUIDSerializer.get());
        finish.addComponent(Character.toString(Character.MAX_VALUE), StringSerializer.get());

        query.setRange(start, finish, false, Integer.MAX_VALUE);

        Iterator<HColumn<Composite, String>> iterator = query.execute().get().getColumns().iterator();
        Set<String> result = new HashSet<String>();
        while (iterator.hasNext()) {
            HColumn<Composite, String> c = iterator.next();
            result.add(c.getValue());
        }
        return result.iterator();
    }

    private boolean contains(ColumnFamilyDefinition columnFamilyDefinition, String keyspace) {
        List<ColumnFamilyDefinition> columnFamilyDefinitions = cluster.describeKeyspace(keyspace).getCfDefs();
        for (ColumnFamilyDefinition def : columnFamilyDefinitions) {
            if (def.getName().equals(columnFamilyDefinition.getName())) {
                return true;
            }
        }

        return false;
    }
}
