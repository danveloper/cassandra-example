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
             for(Iterator<String> it= source.sensorNameIterator(company,startMs,endMs);it.hasNext();){
                String sensor = it.next();
                Iterator<HCounterColumn<UUID>> iterator = source.dataPointIterator(company, sensor, startMs, endMs);

                int count = 0;
                int sum = 0;
                for (; iterator.hasNext(); ) {
                    HCounterColumn<UUID> columns = iterator.next();
                    long value = columns.getValue();
                    sum+=value;
                    count++;
                }

                 long avg = count==0?0:sum/count;
                target.insert(company, sensor, startMs, avg);
            }
        } catch (Throwable e) {
            logger.warn("Failed to do a rollup", e);
        }
    }
}
