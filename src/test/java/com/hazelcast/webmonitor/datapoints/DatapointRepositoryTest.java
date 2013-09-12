package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.datapoints.DatapointRepository;
import com.hazelcast.webmonitor.datapoints.Datapoint;
import com.hazelcast.webmonitor.repositories.AbstractRepositoryTest;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DatapointRepositoryTest extends AbstractRepositoryTest {

    private DatapointRepository datapointRepository;

    @Before
    public void setUp() {
        super.setUp();
        datapointRepository = new DatapointRepository(cluster, keyspace,"datapoints",1);
    }

    @Test
    public void slice() throws InterruptedException {
        Datapoint datapoint = createDatapoint();
        datapointRepository.insert(datapoint);

        System.out.println(datapoint);

        List<Datapoint> result = datapointRepository.slice(datapoint.company,datapoint.cluster,null,datapoint.id,datapoint.metricName, datapoint.timestampMs - 1, datapoint.timestampMs + 1);

        assertEquals(1, result.size());

        System.out.println(datapoint);

        System.out.println(result.get(0));

        assertTrue(result.contains(datapoint));
    }

    @Test
    public void sliceWithMember() {
        Datapoint datapoint1 = createDatapoint();
        Datapoint datapoint2 = new Datapoint(datapoint1);
        datapoint2.member="192.168.1.2";
        Datapoint datapoint3 = new Datapoint(datapoint1);
        datapoint3.member="192.168.1.3";

        datapointRepository.insert(datapoint1);
        datapointRepository.insert(datapoint1);
        datapointRepository.insert(datapoint1);

        List<Datapoint> result = datapointRepository.slice(datapoint1.company, datapoint1.cluster, datapoint1.member, null, datapoint1.metricName, datapoint1.timestampMs - 1, datapoint1.timestampMs + 1);

        assertEquals(1, result.size());
        assertTrue(result.contains(datapoint1));
    }

    public void print(List<Datapoint> datapoints){
        for(Datapoint datapoint: datapoints){
            System.out.println(datapoint);
        }
    }

    public Datapoint createDatapoint() {
        Datapoint datapoint = new Datapoint();
        datapoint.metricName = "IMap.readCount";
        datapoint.maximum = 500;
        datapoint.minimum = 200;
        datapoint.avg = 10;
        datapoint.timestampMs = System.currentTimeMillis();
        datapoint.cluster = "dev";
        datapoint.member = "192.168.1.1";
        datapoint.id = "map1";
        datapoint.company = "hazelcast";
        return datapoint;
    }
}
