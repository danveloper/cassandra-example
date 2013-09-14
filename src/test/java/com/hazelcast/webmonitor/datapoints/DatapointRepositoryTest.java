package com.hazelcast.webmonitor.datapoints;

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
        Datapoint p = createDatapoint();
        datapointRepository.insert(p);

        System.out.println(p);

        List<Datapoint> result = datapointRepository.slice(p.company,p.cluster,null,p.id,p.metricName, p.timestampMs - 1, p.timestampMs + 1);

        assertEquals(1, result.size());

        System.out.println(p);

        System.out.println(result.get(0));

        assertTrue(result.contains(p));
    }

    @Test
    public void sliceWithMember() {
        Datapoint p1 = createDatapoint();
        Datapoint datapoint2 = new Datapoint(p1);
        datapoint2.member="192.168.1.2";
        Datapoint datapoint3 = new Datapoint(p1);
        datapoint3.member="192.168.1.3";

        datapointRepository.insert(p1);
        datapointRepository.insert(p1);
        datapointRepository.insert(p1);

        List<Datapoint> result = datapointRepository.slice(p1.company, p1.cluster, p1.member, null, p1.metricName, p1.timestampMs - 1, p1.timestampMs + 1);

        assertEquals(1, result.size());
        assertTrue(result.contains(p1));
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
        datapoint.average = 10;
        datapoint.velocity = 40;
        datapoint.timestampMs = System.currentTimeMillis();
        datapoint.cluster = "dev";
        datapoint.member = "192.168.1.1";
        datapoint.id = "map1";
        datapoint.company = "hazelcast";
        return datapoint;
    }
}
