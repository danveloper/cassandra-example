package com.hazelcast.webmonitor.scheduler;

import com.eaio.uuid.UUID;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.log4j.Logger;

import java.util.Iterator;

public class VelocityRollupRunnable extends AbstractRollupRunnable {

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
        Long first = null;
        long last = 0;
        int count = 0;
        for ( Iterator<HCounterColumn<UUID>> datapointIt = source.dataPointIterator(company, sensor, startMs, endMs); datapointIt.hasNext(); ) {
            HCounterColumn<UUID> columns = datapointIt.next();
            long value = columns.getValue();
            if(first ==null){
                first = value;
            }
            last = value;

            count++;
        }

        if (count > 0) {
            long velocity = (1000*(last-first))/(endMs-startMs);
            target.insert(company, sensor, startMs, velocity);
        }
    }
}