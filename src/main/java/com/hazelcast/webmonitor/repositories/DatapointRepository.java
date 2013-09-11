package com.hazelcast.webmonitor.repositories;

import com.eaio.uuid.UUID;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.beans.HCounterColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.CounterQuery;
import me.prettyprint.hector.api.query.SliceCounterQuery;
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.*;

import static me.prettyprint.hector.api.factory.HFactory.createCounterSliceQuery;
import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;


public class DatapointRepository extends AbstractRepository {
    private final int ttlMs;
    private final String name;

    public DatapointRepository(Cluster cluster, Keyspace keyspace, String name, int ttlMs) {
        super(cluster, keyspace);
        this.name = name.toLowerCase();
        this.ttlMs = ttlMs;
    }

    public String getName() {
        return name;
    }

    private String toDatapointCfName(String company) {
        return name + "_" + company;
    }

    private String toSensornameCfName(String company) {
        return name + "_" + company + "_names";
    }

    public void createColumnFamilies(String company) {
        ColumnFamilyDefinition datapointColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), toDatapointCfName(company));
        //Validator to use for keys
        datapointColumnFamily.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
        //Defines how to store, compare and validate the column names
        datapointColumnFamily.setComparatorType(ComparatorType.TIMEUUIDTYPE);
        //Validator to use for values in columns
        datapointColumnFamily.setDefaultValidationClass(ComparatorType.COUNTERTYPE.getClassName());

        ColumnFamilyDefinition sensornameColumnFamily = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), toSensornameCfName(company));
        sensornameColumnFamily.setKeyValidationClass("UTF8Type");
        sensornameColumnFamily.setComparatorTypeAlias("(TimeUUIDType, UTF8Type, UTF8Type)");
        sensornameColumnFamily.setDefaultValidationClass("UTF8Type");

        add(datapointColumnFamily);
        add(sensornameColumnFamily);
    }

    public void insert(String company, String sensor, long timeMs, long value) {
        UUID timeUUID = toTimeUUID(timeMs);

        //increase the value (will insert of non existing, or increase when exists)
        Mutator<String> mutator = HFactory.createMutator(keyspace,
                StringSerializer.get());
        HCounterColumn<UUID> counterColumn = HFactory.createCounterColumn(timeUUID, value, TimeUUIDSerializer.get());
        counterColumn.setTtl(ttlMs);
        mutator.insertCounter(sensor,toDatapointCfName(company),counterColumn);
        mutator.execute();

        //inserts the sensor name
        Composite timenameColumnKey = new Composite();
        timenameColumnKey.addComponent(timeUUID, TimeUUIDSerializer.get());
        timenameColumnKey.addComponent(sensor, StringSerializer.get());
        timenameColumnKey.addComponent(sensor, StringSerializer.get());

        Mutator<String> sensornameMutator = createMutator(keyspace, StringSerializer.get());
        HColumn<Composite, String> sensornameColumn = HFactory.createColumn(
                timenameColumnKey,
                sensor,
                new CompositeSerializer(),
                StringSerializer.get());
        sensornameColumn.setTtl(ttlMs);
        sensornameMutator.addInsertion("name", toSensornameCfName(company), sensornameColumn);
        sensornameMutator.execute();
    }

    public Long read(String company, String sensor, long time) {
        CounterQuery<String, UUID> query = HFactory.createCounterColumnQuery(keyspace, StringSerializer.get(), TimeUUIDSerializer.get());
        query.setColumnFamily(toDatapointCfName(company));
        query.setKey(sensor);
        query.setName(toTimeUUID(time));

        HCounterColumn<UUID> result = query.execute().get();
        return result == null ? null : result.getValue();
    }

    public Map<UUID, Long> selectColumnsBetween(String company, String sensor, long startMs, long endMs) {
        SliceCounterQuery<String, UUID> query = createCounterSliceQuery(keyspace, StringSerializer.get(), TimeUUIDSerializer.get())
                .setKey(sensor)
                .setColumnFamily(toDatapointCfName(company));
        query.setRange(toTimeUUID(startMs), toTimeUUID(endMs),false,Integer.MAX_VALUE);
        Iterator<HCounterColumn<UUID>> iterator = query.execute().get().getColumns().iterator();

        LinkedHashMap<UUID, Long> result = new LinkedHashMap<UUID, Long>();
        while (iterator.hasNext()) {
            HCounterColumn<UUID> c = iterator.next();
            result.put(c.getName(), c.getValue());
        }
        return result;
    }

    public Iterator<HCounterColumn<UUID>> dataPointIterator(String company, String sensor, long startMs, long endMs) {
        SliceCounterQuery<String, UUID> query = createCounterSliceQuery(keyspace, StringSerializer.get(), TimeUUIDSerializer.get())
                .setKey(sensor)
                .setColumnFamily(toDatapointCfName(company));
        query.setRange(toTimeUUID(startMs), toTimeUUID(endMs),false,Integer.MAX_VALUE);
        return query.execute().get().getColumns().iterator();
    }

    public Set<String> sensorNames(String company, long startMs, long endMs){
        Set<String> s = new HashSet<String>();
        for(Iterator<String> it = sensorNameIterator(company,startMs,endMs);it.hasNext();){
            s.add(it.next());
        }
        return s;
    }

    public Iterator<String> sensorNameIterator(String company, long startMs, long endMs) {
        String beginString = Character.toString(Character.MIN_VALUE);
        String endString = Character.toString(Character.MAX_VALUE);


        Composite begin = new Composite();
        begin.addComponent(toTimeUUID(startMs), TimeUUIDSerializer.get());
        begin.addComponent(beginString, StringSerializer.get());

        Composite end = new Composite();
        end.addComponent(toTimeUUID(endMs), TimeUUIDSerializer.get());
        end.addComponent(endString, StringSerializer.get());

        SliceQuery<String, Composite, String> query = createSliceQuery(keyspace, StringSerializer.get(),
                CompositeSerializer.get(),
                StringSerializer.get());
        query.setColumnFamily(toSensornameCfName(company));
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

}
