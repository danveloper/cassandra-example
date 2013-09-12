package com.hazelcast.webmonitor.repositories;

import com.hazelcast.webmonitor.newdatapoint.Datapoint;
import com.hazelcast.webmonitor.repositories.AbstractRepository;
import com.hazelcast.webmonitor.repositories.CassandraUtils;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.cassandra.serializers.TimeUUIDSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;


/**
 * Perhaps use a column family
 * time: timestamp
 * cluster: dev
 * machine: 192.168.1.1
 * <p/>
 * And get a secondary index on machine so you can easily lookup all machines
 * <p/>
 * <p/>
 * http://www.datastax.com/dev/blog/metric-collection-and-storage-with-cassandra
 */
public class DatapointRepository extends AbstractRepository {
    private final static String beginString = Character.toString(Character.MIN_VALUE);
    private final static String endString = Character.toString(Character.MAX_VALUE);

    private final ColumnFamilyDefinition cf;
    private final int rollupPeriodMs;

    public DatapointRepository(Cluster cluster, Keyspace keyspace, String tableName, int rollupPeriodMs) {
        super(cluster, keyspace);

        this.rollupPeriodMs = rollupPeriodMs;

        cf = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), tableName);
        //Validator to use for keys
        cf.setKeyValidationClass(ComparatorType.UTF8TYPE.getClassName());
        //Defines how to store, compare and validate the column names
        //first element is timestamp, second element is member, third element is the id
        cf.setComparatorTypeAlias("(LongType, UTF8Type, UTF8Type, UTF8Type, UTF8Type, LongType)");
        //Validator to use for values in columns
        cf.setDefaultValidationClass(ComparatorType.LONGTYPE.getClassName());

        add(cf);
    }

    public int getRollupPeriodMs() {
        return rollupPeriodMs;
    }

    public void insert(Datapoint datapoint) {
        String rowKey = datapoint.metricName;

        long timeMs = rollupPeriodMs * (datapoint.timestampMs / rollupPeriodMs);

        Composite columnKey = new Composite();
        columnKey.addComponent(timeMs, LongSerializer.get());
        columnKey.addComponent(datapoint.member, StringSerializer.get());
        columnKey.addComponent(datapoint.id, StringSerializer.get());
        columnKey.addComponent(datapoint.company, StringSerializer.get());
        columnKey.addComponent(datapoint.cluster, StringSerializer.get());
        //todo:this should not be needed but currently I'm not able to convert the first column back to a timestamp
        columnKey.addComponent(timeMs, LongSerializer.get());

        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        HColumn<Composite, Long> column = HFactory.createColumn(
                columnKey,
                datapoint.value,
                CompositeSerializer.get(),
                LongSerializer.get());
        mutator.addInsertion(rowKey, cf.getName(), column);
        mutator.execute();
    }

    public List<Datapoint> slice(String metricName, long startMs, long endMs) {
        String rowKey = metricName;

        Composite begin = new Composite();
        begin.addComponent(startMs, LongSerializer.get());

        Composite end = new Composite();
        end.addComponent(endMs, LongSerializer.get());

        SliceQuery<String, Composite, Long> query = createSliceQuery(keyspace, StringSerializer.get(),
                CompositeSerializer.get(),
                LongSerializer.get());
        query.setColumnFamily(cf.getName());
        query.setKey(rowKey);
        query.setRange(begin, end, false, Integer.MAX_VALUE);

        Iterator<HColumn<Composite, Long>> iterator = query.execute().get().getColumns().iterator();
        List<Datapoint> result = new LinkedList<Datapoint>();
        while (iterator.hasNext()) {
            Datapoint datapoint = getDatapoint(metricName, iterator.next());
            result.add(datapoint);
        }
        return result;
    }

    public List<Datapoint> sliceForMember(String metricName, String member, long startMs, long endMs) {
        String rowKey = metricName;

        Composite begin = new Composite();

        begin.addComponent(startMs, LongSerializer.get());
        begin.addComponent(member, StringSerializer.get());

        Composite end = new Composite();
        end.addComponent(endMs, LongSerializer.get());
        end.addComponent(member, StringSerializer.get());

        SliceQuery<String, Composite, Long> query = createSliceQuery(keyspace, StringSerializer.get(),
                CompositeSerializer.get(),
                LongSerializer.get());
        query.setColumnFamily(cf.getName());
        query.setKey(rowKey);
        query.setRange(begin, end, false, Integer.MAX_VALUE);

        Iterator<HColumn<Composite, Long>> iterator = query.execute().get().getColumns().iterator();
        LinkedList<Datapoint> result = new LinkedList<Datapoint>();
        while (iterator.hasNext()) {
            Datapoint datapoint = getDatapoint(metricName, iterator.next());
            result.add(datapoint);
        }
        return result;
    }


    private Datapoint getDatapoint(String metricName, HColumn<Composite, Long> hcolumn) {
        Composite column = hcolumn.getName();
        Datapoint datapoint = new Datapoint();
        datapoint.metricName = metricName;

        datapoint.value = hcolumn.getValue();

        datapoint.member = column.get(1, StringSerializer.get());
        datapoint.id = column.get(2, StringSerializer.get());
        datapoint.company = column.get(3, StringSerializer.get());
        datapoint.cluster = column.get(4, StringSerializer.get());
        //todo: delete once we are able to retrieve time from first column
        datapoint.timestampMs = column.get(5, LongSerializer.get());
        return datapoint;
    }

}
