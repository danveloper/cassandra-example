package com.hazelcast.webmonitor.repositories;

import com.eaio.uuid.UUID;
import me.prettyprint.cassandra.serializers.CompositeSerializer;
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

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import static me.prettyprint.hector.api.factory.HFactory.createMutator;
import static me.prettyprint.hector.api.factory.HFactory.createSliceQuery;

/**
 * todo: by using a mod in the time, you can save space.
 * we don't need to know at the millisecond if a member was or was not there. It is fine if we know it for 5 seconds for example.
 * so by doing a (div 5000)*5000, all 'inserts' within a certain period will overwrite the entry
 * <p/>
 * If a list of members is also maintained locally, one could check the memory if a member was written a little time back.
 * if it was already inserted, then there is no reason to insert it again.
 * <p/>
 */
public class ActiveMembersRepository extends AbstractRepository {

    private final ColumnFamilyDefinition columnFamily;

    public ActiveMembersRepository(Cluster cluster, Keyspace keyspace) {
        super(cluster, keyspace);

        columnFamily = HFactory.createColumnFamilyDefinition(keyspace.getKeyspaceName(), "ActiveMembers",ComparatorType.COMPOSITETYPE);
        //Validator to use for keys
        columnFamily.setKeyValidationClass("UTF8Type");
        //Defines how to store, compare and validate the column names
        columnFamily.setComparatorTypeAlias("(TimeUUIDType, UTF8Type)");

        //Validator to use for values in columns
        columnFamily.setDefaultValidationClass("UTF8Type");

        add(columnFamily);
    }

    public void insert(String company, String cluster, String member, long timeMs) {
        String rowKey = company + "!" + cluster;

        UUID timeUUID = toTimeUUID(timeMs);

        //inserts the sensor name
        Composite columnKey = new Composite();
        columnKey.addComponent(timeUUID, TimeUUIDSerializer.get());
        columnKey.addComponent(member, StringSerializer.get());

        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        HColumn<Composite, String> column = HFactory.createColumn(
                columnKey,
                member,
                new CompositeSerializer(),
                StringSerializer.get());
        mutator.addInsertion(rowKey, columnFamily.getName(), column);
        mutator.execute();
    }

    public Set<String> getActiveMembers(String company, String cluster, long startMs, long endMs) {
        String rowKey = company + "!" + cluster;

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
        query.setColumnFamily(columnFamily.getName());
        query.setKey(rowKey);
        query.setRange(begin, end, false, Integer.MAX_VALUE);

        Iterator<HColumn<Composite, String>> iterator = query.execute().get().getColumns().iterator();
        Set<String> result = new HashSet<String>();
        while (iterator.hasNext()) {
            HColumn<Composite, String> c = iterator.next();
            result.add(c.getValue());
        }
        return result;
    }
}
