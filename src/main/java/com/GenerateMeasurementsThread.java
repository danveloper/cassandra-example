package com;

import java.util.Random;
import java.util.concurrent.BlockingQueue;

public class GenerateMeasurementsThread extends Thread {

    private final String[] clusters = new String[]{"dev"};
    private final String[][] machines = new String[][]{
            new String[]{"192.168.1.1:5701","192.168.1.2:5701"}};

    private final String[] maps = new String[]{"map1"};
    private final long[] readCounters = new long[maps.length];
    private final long[] writeCounters = new long[maps.length];
    private final BlockingQueue<Measurement> measurementQueue;
    private final static Random random = new Random();
    private final String customer;

    public GenerateMeasurementsThread(BlockingQueue<Measurement> measurementQueue, String customer) {
        this.measurementQueue = measurementQueue;
        this.customer = customer;
    }

    public void run() {
        Random random = new Random();

        for (; ; ) {
            try {
                Thread.sleep(random.nextInt(10));
            } catch (InterruptedException e) {
            }

            Measurement measurement = createMeasurement();
            measurementQueue.add(measurement);
        }
    }

    public Measurement createMeasurement() {
        Measurement measurement = new Measurement();
        measurement.customer = customer;
        int clusterIndex = random.nextInt(clusters.length);
        measurement.environment = clusters[clusterIndex];
        String[] machinesForEnvironment = machines[clusterIndex];
        measurement.machine = machinesForEnvironment[random.nextInt(machinesForEnvironment.length)];
        measurement.timeMs = System.currentTimeMillis();
        int mapIndex = random.nextInt(maps.length);
        measurement.subject = maps[mapIndex];

        long readCount = readCounters[mapIndex];
        readCount += random.nextInt(100);
        readCounters[mapIndex] = readCount;

        long writeCount = writeCounters[mapIndex];
        writeCount += random.nextInt(50);
        writeCounters[mapIndex] = writeCount;

        measurement.map.put("readCount", readCount);
        //measurement.map.put("writeCount",writeCount);
        return measurement;
    }
}
