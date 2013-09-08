package com;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class GenerateMeasurementsThread extends Thread {
    private final String machine;
    private final DatapointRepository datapointRepository;
    private final static AtomicLong operationCount = new AtomicLong();
    private final static Random random = new Random();
    private final String customer;

    public GenerateMeasurementsThread(DatapointRepository datapointRepository, String customer, String machine) {
        this.datapointRepository = datapointRepository;
        this.machine = machine;
        this.customer = customer;
    }

    public void run() {
        Random random = new Random();

        for (; ; ) {
            try {
                Thread.sleep(random.nextInt(100));
            } catch (InterruptedException e) {
            }

            Measurement measurement = createMeasurement();

            for (Map.Entry<String, String> entry : measurement.map.entrySet()) {
                datapointRepository.update(customer, machine + ":" + entry.getKey(), measurement.timeMs, entry.getValue());
            }
        }
    }

    public static Measurement createMeasurement() {
        Measurement measurement = new Measurement();
        measurement.timeMs = System.currentTimeMillis();
        measurement.map.put("operationXCount", "" + operationCount.addAndGet(random.nextInt(100)));
        measurement.map.put("operationYCount", "" + operationCount.addAndGet(random.nextInt(100)));
        return measurement;

    }
}
