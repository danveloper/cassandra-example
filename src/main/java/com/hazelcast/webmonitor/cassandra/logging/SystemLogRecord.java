package com.hazelcast.webmonitor.cassandra.logging;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

//todo: can be removed once hazelcast jar is added.
public class SystemLogRecord implements Comparable {

    private long date;
    private String node;
    private String message;
    private String type;

    public SystemLogRecord(long date, String message, String type) {
        this.date = date;
        this.message = message;
        this.type = type;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public SystemLogRecord() {
    }

    public long getDate() {
        return date;
    }

    public void setDate(long date) {
        this.date = date;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int compareTo(Object o) {
        long thisVal = this.date;
        SystemLogRecord other = (SystemLogRecord) o;
        long anotherVal = other.getDate();
        return (thisVal < anotherVal ? -1 : (thisVal == anotherVal ? 0 : 1));
    }

    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeLong(date);
        out.writeUTF(message);
        out.writeUTF(type);
    }

    public void readData(ObjectDataInput in) throws IOException {
        date = in.readLong();
        message = in.readUTF();
        type = in.readUTF();
    }

    @Override
    public String toString() {
        return "SystemLogRecord{" +
                "date=" + date +
                ", node='" + node + '\'' +
                ", message='" + message + '\'' +
                ", type='" + type + '\'' +
                '}';
    }
}
