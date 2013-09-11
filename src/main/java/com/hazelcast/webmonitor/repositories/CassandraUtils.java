package com.hazelcast.webmonitor.repositories;

import me.prettyprint.cassandra.serializers.StringSerializer;
import org.apache.cassandra.thrift.ColumnDef;
import org.apache.cassandra.thrift.IndexType;

import java.nio.ByteBuffer;

public class CassandraUtils {

    public static ColumnDef newIndexedColumnDef(String columnName, String comparator) {
        ColumnDef cd = newColumnDef(columnName, comparator);
        cd.setIndex_name(columnName);
        cd.setIndex_type(IndexType.KEYS);
        return cd;
    }

    public static ColumnDef newColumnDef(String columnName, String comparator) {
        return new ColumnDef(toByteBuffer(columnName), comparator);
    }

    public static ByteBuffer toByteBuffer(String msg) {
        return StringSerializer.get().toByteBuffer(msg);
    }

    private CassandraUtils() {
    }

}
