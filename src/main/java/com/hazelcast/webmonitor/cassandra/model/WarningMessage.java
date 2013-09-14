package com.hazelcast.webmonitor.cassandra.model;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.DataSerializable;

import java.io.IOException;

public class WarningMessage implements DataSerializable {
    private String company;
    private String date;
    private String message;
    private String clusterName;

    public WarningMessage() {

    }

    public WarningMessage(String company, String date, String message, String clusterName) {
        this.company = company;
        this.date = date;
        this.message = message;
        this.clusterName = clusterName;
    }

    public String getCompany() {
        return company;
    }

    public String getDate() {
        return date;
    }

    public String getMessage() {
        return message;
    }

    public String getClusterName() {
        return clusterName;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeUTF(company);
        out.writeUTF(date);
        out.writeUTF(message);
        out.writeUTF(clusterName);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        company = in.readUTF();
        date = in.readUTF();
        message = in.readUTF();
        clusterName = in.readUTF();
    }

    @Override
    public String toString() {
        return "WarningMessage{" +
                "company='" + company + '\'' +
                ", date='" + date + '\'' +
                ", message='" + message + '\'' +
                ", clusterName='" + clusterName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        WarningMessage that = (WarningMessage) o;

        if (company != null ? !company.equals(that.company) : that.company != null) return false;
        if (clusterName != null ? !clusterName.equals(that.clusterName) : that.clusterName != null) return false;
        if (date != null ? !date.equals(that.date) : that.date != null) return false;
        if (message != null ? !message.equals(that.message) : that.message != null) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = date != null ? date.hashCode() : 0;
        result = 31 * result + (company != null ? company.hashCode() : 0);
        result = 31 * result + (message != null ? message.hashCode() : 0);
        result = 31 * result + (clusterName != null ? clusterName.hashCode() : 0);
        return result;
    }
}
