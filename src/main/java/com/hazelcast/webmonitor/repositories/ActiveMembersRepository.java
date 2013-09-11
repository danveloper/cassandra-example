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

import static me.prettyprint.hector.api.factory.HFactory.*;

/**
 * todo: by using a mod in the time, you can save space.
 * we don't need to know at the millisecond if a member was or was not there. It is fine if we know it for 5 seconds for example.
 * so by doing a (div 5000)*5000, all 'inserts' within a certain period will overwrite the entry
 * <p/>
 * If a list of members is also maintained locally, one could check the memory if a member was written a little time back.
 * if it was already inserted, then there is no reason to insert it again.
 * <p/>
 * <p/>
 * We could also do some compaction on this raw repository.
 */
public class ActiveMembersRepository extends AbstractRepository {

    private final ColumnFamilyDefinition activeMemberColDef;
    private final ColumnFamilyDefinition activeClusterColDef;

    public ActiveMembersRepository(Cluster cluster, Keyspace keyspace){
        this(cluster, keyspace, "");
    }

    public ActiveMembersRepository(Cluster cluster, Keyspace keyspace, String cfPrefix) {
        super(cluster, keyspace);

        activeMemberColDef = createColumnFamilyDefinition(keyspace.getKeyspaceName(), cfPrefix+"active_members");
        //Validator to use for keys
        activeMemberColDef.setKeyValidationClass("UTF8Type");
        //Defines how to store, compare and validate the column names
        activeMemberColDef.setComparatorTypeAlias("(TimeUUIDType, UTF8Type)");
        //Validator to use for values in columns
        activeMemberColDef.setDefaultValidationClass("UTF8Type");
        add(activeMemberColDef);

        activeClusterColDef = createColumnFamilyDefinition(keyspace.getKeyspaceName(), cfPrefix+"active_clusters");
        //Validator to use for keys
        activeClusterColDef.setKeyValidationClass("UTF8Type");
        //Defines how to store, compare and validate the column names
        activeClusterColDef.setComparatorType(ComparatorType.TIMEUUIDTYPE);
        //Validator to use for values in columns
        activeClusterColDef.setDefaultValidationClass("UTF8Type");
        add(activeClusterColDef);
    }

    public void insert(String company, String cluster, String member, long timeMs) {
        //timeMs = (timeMs / 1000)*timeMs;
        UUID timeUUID = toTimeUUID(timeMs);

        insertMember(company, cluster, member, timeUUID);
        insertCluster(company, cluster, timeUUID);
    }

    private void insertMember(String company, String cluster, String member, UUID timeUUID) {
        String rowKey = company + "!" + cluster;

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
        mutator.addInsertion(rowKey, activeMemberColDef.getName(), column);
        mutator.execute();
    }

    private void insertCluster(String company, String cluster, UUID timeUUID) {
        String rowKey = company;

        Mutator<String> mutator = createMutator(keyspace, StringSerializer.get());
        HColumn<UUID, String> column = HFactory.createColumn(
                timeUUID,
                cluster,
                TimeUUIDSerializer.get(),
                StringSerializer.get());
        mutator.addInsertion(rowKey, activeClusterColDef.getName(), column);
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
        query.setColumnFamily(activeMemberColDef.getName());
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

    public Set<String> getActiveClusters(String company, long startMs, long endMs) {
        String rowKey = company;

        SliceQuery<String, UUID, String> query = createSliceQuery(keyspace, StringSerializer.get(),
                TimeUUIDSerializer.get(),
                StringSerializer.get());
        query.setColumnFamily(activeClusterColDef.getName());
        query.setKey(rowKey);
        query.setRange(toTimeUUID(startMs), toTimeUUID(endMs), false, Integer.MAX_VALUE);

        Iterator<HColumn<UUID, String>> iterator = query.execute().get().getColumns().iterator();
        Set<String> result = new HashSet<String>();
        while (iterator.hasNext()) {
            HColumn<UUID, String> c = iterator.next();
            result.add(c.getValue());
        }
        return result;
    }
}
