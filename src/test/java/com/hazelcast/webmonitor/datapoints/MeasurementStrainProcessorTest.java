package com.hazelcast.webmonitor.datapoints;

import com.hazelcast.webmonitor.Measurement;
import com.hazelcast.webmonitor.repositories.AbstractRepositoryTest;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class MeasurementStrainProcessorTest extends AbstractRepositoryTest {

    @Before
    public void setUp(){
        super.setUp();
    }

    @Test
    public void maxHistory1() {
        DatapointRepository repository = new DatapointRepository(cluster, keyspace, "datapoints", 1000);
        MeasurementStrainProcessor task = new MeasurementStrainProcessor(repository);
        assertEquals(1000, task.rollupPeriodMs());
        assertEquals(2, task.getHistoryLength());
    }

    @Test
    public void maxHistory5() {
        DatapointRepository repository = new DatapointRepository(cluster, keyspace, "datapoints", 5000);
        MeasurementStrainProcessor task = new MeasurementStrainProcessor(repository);
        assertEquals(5000, task.rollupPeriodMs());
        assertEquals(6, task.getHistoryLength());
    }

    @Test
    public void maxHistory60() {
        DatapointRepository repository = new DatapointRepository(cluster, keyspace, "datapoints", 60000);
        MeasurementStrainProcessor task = new MeasurementStrainProcessor(repository);
        assertEquals(60000, task.rollupPeriodMs());
        assertEquals(10, task.getHistoryLength());
    }

    @Test
    public void testSecond() {
        DatapointRepository repository = new DatapointRepository(cluster, keyspace, "datapoints", 1000);
        final MeasurementStrainProcessor task = new MeasurementStrainProcessor(repository);

        Measurement measurement1 = new Measurement();
        measurement1.value=10;
        measurement1.metricName="readCount";
        measurement1.company="hazelcast";
        measurement1.id="map1";
        measurement1.cluster="dev";
        measurement1.member="192.168.1.1:5701";
        measurement1.timestampMs=1000;
        task.process(new MeasurementStrain(1000,measurement1));

        Measurement measurement2 = new Measurement(measurement1);
        measurement2.timestampMs=2000;
        measurement2.value=20;
        task.process(new MeasurementStrain(2000, measurement2));

        Measurement measurement3 = new Measurement(measurement1);
        measurement3.timestampMs=3000;
        measurement3.value=30;
        task.process(new MeasurementStrain(3000, measurement3));

        Measurement measurement4 = new Measurement(measurement1);
        measurement4.timestampMs=4000;
        measurement4.value=40;
        task.process(new MeasurementStrain(4000,measurement4));

        System.out.println("foo");
    }

    @Test
    public void test10Second() {
        DatapointRepository repository = new DatapointRepository(cluster, keyspace, "datapoints", 10000);
        final MeasurementStrainProcessor task = new MeasurementStrainProcessor(repository);

        Measurement measurement1 = new Measurement();
        measurement1.value=10;
        measurement1.metricName="readCount";
        measurement1.company="hazelcast";
        measurement1.id="map1";
        measurement1.cluster="dev";
        measurement1.member="192.168.1.1:5701";
        measurement1.timestampMs=10000;
        task.process(new MeasurementStrain(measurement1.timestampMs,measurement1));

        Measurement measurement2 = new Measurement(measurement1);
        measurement2.timestampMs=20000;
        measurement2.value=20;
        task.process(new MeasurementStrain(measurement2.timestampMs,measurement2));

        Measurement measurement3 = new Measurement(measurement1);
        measurement3.timestampMs=30000;
        measurement3.value=30;
        task.process(new MeasurementStrain(measurement3.timestampMs,measurement3));

        Measurement measurement4 = new Measurement(measurement1);
        measurement4.timestampMs=40000;
        measurement4.value=40;
        task.process(new MeasurementStrain(measurement4.timestampMs,measurement4));

        System.out.println("foo");
    }
}
