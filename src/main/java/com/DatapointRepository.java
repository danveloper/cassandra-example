package com;

import com.eaio.uuid.UUID;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.cassandra.service.ColumnSliceIterator;
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
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.*;

import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;


public class DatapointRepository {
    private final Keyspace keyspace;
    private final Cluster cluster;

    private final int ttlMs;
    private final String name;

    public DatapointRepository(Cluster cluster, Keyspace Keyspace, String name, int ttlMs) {
        this.name = name.toLowerCase();
        this.cluster = cluster;
        this.keyspace = Keyspace;
        this.ttlMs = ttlMs;
    }

    private void addIfNotExist(Cluster cluster, Keyspace keyspace, ColumnFamilyDefinition def) {
        //todo:racy
        if (!contains(def, keyspace.getKeyspaceName())) {
            cluster.addColumnFamily(def);
        }
    }

    private String toDatapointCfName(String customer) {
        return name + "_" + customer;
    }

    private String toSensornamesCfName(String customer) {
        return name + "_" + customer + "_names";
    }

    public static UUID toTimeUUID(long time) {
        return new UUID(TimeUUIDUtils.getTimeUUID(time).toString());
    }

    public void createColumnFamilies(String customer) {
        ColumnFamilyDefinition datapointColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), toDatapointCfName(customer), ComparatorType.UUIDTYPE);

        addIfNotExist(cluster, keyspace, datapointColumnFamily);

        ColumnFamilyDefinition sensornameColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), toSensornamesCfName(customer), ComparatorType.COMPOSITETYPE);
        sensornameColumnFamily.setComparatorTypeAlias("(TimeUUIDType, UTF8Type)");
        sensornameColumnFamily.setKeyValidationClass("UTF8Type");
        sensornameColumnFamily.setDefaultValidationClass("UTF8Type");

        addIfNotExist(cluster, keyspace, sensornameColumnFamily);
    }

    public void update(String customer, String sensor, long timeMs, String value) {
        UUID timeUUID = toTimeUUID(timeMs);

        //inserts the sensor value
        Mutator<String> datapointMutator = createMutator(keyspace, StringSerializer.get());
        HColumn<UUID, String> datapointColumn = HFactory.createColumn(
                timeUUID,
                value,
                TimeUUIDSerializer.get(),
                StringSerializer.get());
        datapointColumn.setTtl(ttlMs);
        datapointMutator.addInsertion(sensor, toDatapointCfName(customer), datapointColumn);
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
        sensornameMutator.addInsertion("name", toSensornamesCfName(customer), sensornameColumn);
        sensornameMutator.execute();
    }

    public String read(String customer, String sensor, long time) {
        ColumnQuery<String, UUID, String> query = HFactory.createColumnQuery(keyspace, StringSerializer.get(), TimeUUIDSerializer.get(), StringSerializer.get());
        query.setColumnFamily(toDatapointCfName(customer));
        query.setKey(sensor);
        query.setName(toTimeUUID(time));

        HColumn<UUID, String> result = query.execute().get();
        return result == null ? null : result.getValue();
    }

    public void delete(String customer, String sensor, long time) {
        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        mutator.delete(sensor, toDatapointCfName(customer), toTimeUUID(time), TimeUUIDSerializer.get());
        mutator.execute();

        //todo: the sensor name should be deleted.
    }

    public void deleteSensor(String customer, String sensor) {
        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        mutator.addDeletion(sensor, toDatapointCfName(customer));
        mutator.execute();
    }

    public Map<UUID, String> selectColumnsBetween(String customer, String sensor, long startMs, long endMs) {
        SliceQuery<String, UUID, String> query = createSliceQuery(keyspace, StringSerializer.get(), TimeUUIDSerializer.get(), StringSerializer.get())
                .setKey(sensor)
                .setColumnFamily(toDatapointCfName(customer));

        ColumnSliceIterator<String, UUID, String> iterator =
                new ColumnSliceIterator<String, UUID, String>(query, toTimeUUID(startMs), toTimeUUID(endMs), false);

        LinkedHashMap<UUID, String> result = new LinkedHashMap<UUID, String>();
        while (iterator.hasNext()) {
            HColumn<UUID, String> c = iterator.next();
            result.put(c.getName(), c.getValue());
        }
        return result;
    }

    public ColumnSliceIterator<String, UUID, String> dataPointIterator(String customer, String sensor, long startMs, long endMs) {
        SliceQuery<String, UUID, String> query = createSliceQuery(keyspace, StringSerializer.get(), TimeUUIDSerializer.get(), StringSerializer.get())
                .setKey(sensor)
                .setColumnFamily(toDatapointCfName(customer));

        return new ColumnSliceIterator<String, UUID, String>(query, toTimeUUID(startMs), toTimeUUID(endMs), false);
    }

    public Iterator<String> sensorNameIterator(String customer, long startMs, long endMs) {
        Composite begin = new Composite();
        begin.addComponent(toTimeUUID(startMs), TimeUUIDSerializer.get());
        String beginString = "a";
        begin.addComponent(beginString, StringSerializer.get());

        Composite end = new Composite();
        end.addComponent(toTimeUUID(endMs), TimeUUIDSerializer.get());
        String endString = Character.toString(Character.MAX_VALUE);
        end.addComponent(endString, StringSerializer.get());

        SliceQuery<String, Composite, String> query = createSliceQuery(keyspace, StringSerializer.get(),
                CompositeSerializer.get(),
                StringSerializer.get());
        query.setColumnFamily(toSensornamesCfName(customer));
        query.setKey("name");
        query.setRange(begin, end, false, Integer.MAX_VALUE);

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
