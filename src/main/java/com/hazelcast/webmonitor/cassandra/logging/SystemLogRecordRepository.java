package com.hazelcast.webmonitor.cassandra.logging;

import com.hazelcast.webmonitor.repositories.AbstractRepository;
import me.prettyprint.hector.api.Cluster;
import me.prettyprint.hector.api.Keyspace;

import java.util.LinkedList;
import java.util.List;

public class SystemLogRecordRepository extends AbstractRepository {

    public SystemLogRecordRepository(Cluster cluster, Keyspace keyspace) {
        super(cluster, keyspace);
    }

    public void insert(String company, String cluster, String member, SystemLogRecord record) {

    }

    public List<SystemLogRecord> slice(SystemLogRecordQuery query) {
        return new LinkedList<SystemLogRecord>();
    }
}
