package com.hazelcast.webmonitor.repositories;

import com.hazelcast.webmonitor.newdatapoint.Datapoint;
import com.hazelcast.webmonitor.newdatapoint.NewDatapointRepository;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class NewDatapointRepositoryTest extends AbstractRepositoryTest {

    private NewDatapointRepository datapointRepository;

    @Before
    public void setUp() {
        super.setUp();
        datapointRepository = new NewDatapointRepository(cluster, keyspace,"datapoints",1000);
    }

    @Test
    public void slice() throws InterruptedException {
        Datapoint datapoint = createDatapoint();
        datapointRepository.insert(datapoint);

        System.out.println(datapoint);

        List<Datapoint> result = datapointRepository.slice(datapoint.metricName, datapoint.timestampMs - 1, datapoint.timestampMs + 1);

        assertEquals(1, result.size());
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

        List<Datapoint> result = datapointRepository.sliceForMember(datapoint1.metricName, datapoint1.member, datapoint1.timestampMs - 1, datapoint1.timestampMs + 1);

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
        datapoint.value = 500;
        datapoint.timestampMs = System.currentTimeMillis();
        datapoint.cluster = "dev";
        datapoint.member = "192.168.1.1";
        datapoint.id = "map1";
        datapoint.company = "hazelcast";
        return datapoint;
    }
}
