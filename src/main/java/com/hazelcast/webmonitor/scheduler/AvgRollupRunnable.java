package com.hazelcast.webmonitor.scheduler;

import com.eaio.uuid.UUID;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.log4j.Logger;

import java.util.Iterator;

public class AvgRollupRunnable extends AbstractRollupRunnable {

    private final static Logger logger = Logger.getLogger(AvgRollupRunnable.class);

    @Override
    public void run() {
        try {
            for (Iterator<String> sensorIt = source.sensorNameIterator(company, startMs, endMs); sensorIt.hasNext(); ) {
                processSensor(sensorIt.next());
            }
        } catch (Throwable e) {
            logger.warn("Failed to do a rollup", e);
        }
    }

    private void processSensor(String sensor) {
        int count = 0;
        int sum = 0;
        for ( Iterator<HCounterColumn<UUID>> datapointIt = source.dataPointIterator(company, sensor, startMs, endMs); datapointIt.hasNext(); ) {
            HCounterColumn<UUID> columns = datapointIt.next();
            long value = columns.getValue();
            sum += value;
            count++;
        }

        if (count > 0) {
            long avg = sum / count;
            target.insert(company, sensor, startMs, avg);
        }
    }
}
