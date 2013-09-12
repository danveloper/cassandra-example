package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.repositories.AbstractRepository;
import me.prettyprint.cassandra.serializers.BytesArraySerializer;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
import me.prettyprint.cassandra.serializers.LongSerializer;
import me.prettyprint.cassandra.serializers.StringSerializer;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;
import me.prettyprint.hector.api.beans.Composite;
import me.prettyprint.hector.api.beans.HColumn;
import me.prettyprint.hector.api.ddl.ColumnFamilyDefinition;
import me.prettyprint.hector.api.ddl.ComparatorType;
import me.prettyprint.hector.api.factory.HFactory;
import me.prettyprint.hector.api.mutation.Mutator;
import me.prettyprint.hector.api.query.SliceQuery;

import java.nio.ByteBuffer;
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
    public final static int LONG_SIZE = 8;

    private final ColumnFamilyDefinition cf;
    private final int rollupPeriodMs;

    public DatapointRepository(Cluster cluster, Keyspace keyspace, String tableName, int rollupPeriodMs) {
        super(cluster, keyspace);

        this.rollupPeriodMs = rollupPeriodMs;

        cf = HFactory.createColumnFamilyDefinition(
                keyspace.getKeyspaceName(), tableName);
        //Validator to use for keys
        cf.setKeyValidationAlias("(UTF8Type, UTF8Type, UTF8Type)");
        //Defines how to store, compare and validate the column names
        //first element is timestamp, second element is member, third element is the id
        cf.setComparatorTypeAlias("(LongType, UTF8Type, UTF8Type, UTF8Type, UTF8Type)");
        //Validator to use for values in columns
        cf.setDefaultValidationClass(ComparatorType.BYTESTYPE.getClassName());

        add(cf);
    }

    public int getRollupPeriodMs() {
        return rollupPeriodMs;
    }

    public void insert(Datapoint datapoint) {
        Composite rowKey = createRowKey(datapoint.company, datapoint.cluster, datapoint.metricName);

        long timestampMs = rollupPeriodMs * (datapoint.timestampMs / rollupPeriodMs);

        Composite columnKey = new Composite();
        columnKey.addComponent(timestampMs, LongSerializer.get());
        columnKey.addComponent(datapoint.member, StringSerializer.get());
        columnKey.addComponent(datapoint.id, StringSerializer.get());

        ByteBuffer value = ByteBuffer.allocate(3 * LONG_SIZE);
        value.putDouble(0, datapoint.maximum);
        value.putDouble(LONG_SIZE, datapoint.minimum);
        value.putDouble(2 * LONG_SIZE, datapoint.avg);

        Mutator<Composite> mutator = createMutator(keyspace, CompositeSerializer.get());
        HColumn<Composite, byte[]> column = HFactory.createColumn(
                columnKey,
                value.array(),
                CompositeSerializer.get(),
                BytesArraySerializer.get());
        mutator.addInsertion(rowKey, cf.getName(), column);
        mutator.execute();
    }

    private Composite createRowKey(String company, String cluster, String metricName) {
        Composite rowKey = new Composite();
        rowKey.addComponent(company, StringSerializer.get());
        rowKey.addComponent(cluster, StringSerializer.get());
        rowKey.addComponent(metricName, StringSerializer.get());
        return rowKey;
    }

    public List<Datapoint> slice(DatapointQuery query) {
        return slice(query.company, query.cluster, query.member, query.id, query.metric, query.beginMs, query.endMs, query.maxResult);
    }

    public List<Datapoint> slice(String company, String cluster, String member, String id, String metricName, long startMs, long endMs) {
        return slice(company, cluster, member, id, metricName, startMs, endMs, Integer.MAX_VALUE);
    }

    public List<Datapoint> slice(String company, String cluster, String member, String id, String metricName, long startMs, long endMs, int maxResult) {
        Composite rowKey = createRowKey(company, cluster, metricName);

        Composite begin = new Composite();
        begin.addComponent(startMs, LongSerializer.get());
        if (member == null) {
            begin.addComponent(AbstractRepository.begin, StringSerializer.get());
        } else {
            begin.addComponent(member, StringSerializer.get());
        }
        if (id == null) {
            begin.addComponent(AbstractRepository.begin, StringSerializer.get());
        } else {
            begin.addComponent(id, StringSerializer.get());
        }

        Composite end = new Composite();
        end.addComponent(endMs, LongSerializer.get());
        if (member == null) {
            end.addComponent(AbstractRepository.end, StringSerializer.get());
        } else {
            end.addComponent(member, StringSerializer.get());
        }
        if (id == null) {
            end.addComponent(AbstractRepository.end, StringSerializer.get());
        } else {
            end.addComponent(id, StringSerializer.get());
        }

        SliceQuery<Composite, Composite, byte[]> query = createSliceQuery(keyspace, CompositeSerializer.get(),
                CompositeSerializer.get(),
                BytesArraySerializer.get());
        query.setColumnFamily(cf.getName());
        query.setKey(rowKey);
        query.setRange(begin, end, false, maxResult);

        Iterator<HColumn<Composite, byte[]>> iterator = query.execute().get().getColumns().iterator();
        List<Datapoint> result = new LinkedList<Datapoint>();
        while (iterator.hasNext()) {
            Datapoint datapoint = getDatapoint(company, cluster, metricName, iterator.next());
            result.add(datapoint);
        }
        return result;
    }


    private Datapoint getDatapoint(String company, String cluster, String metricName, HColumn<Composite, byte[]> hcolumn) {
        Datapoint datapoint = new Datapoint();
        datapoint.metricName = metricName;
        datapoint.company = company;
        datapoint.cluster = cluster;

        //retrieve state from the column-names
        Composite column = hcolumn.getName();
        datapoint.timestampMs = column.get(0, LongSerializer.get());
        datapoint.member = column.get(1, StringSerializer.get());
        datapoint.id = column.get(2, StringSerializer.get());

        //retrieve state from the value
        ByteBuffer byteBuffer = ByteBuffer.wrap(hcolumn.getValue());
        datapoint.maximum = byteBuffer.getDouble(0);
        datapoint.minimum = byteBuffer.getDouble(LONG_SIZE);
        datapoint.avg = byteBuffer.getDouble(2 * LONG_SIZE);

        return datapoint;
    }
}
