package com.hazelcast.webmonitor.scheduler;

import com.eaio.uuid.UUID;
import me.prettyprint.hector.api.beans.HCounterColumn;
import org.apache.log4j.Logger;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class CompactRollupRunnable extends AbstractRollupRunnable {
    private final static Logger logger = Logger.getLogger(CompactRollupRunnable.class);

    @Override
    public void run() {

        try {
            Map<String, Datapoint> datapointMap = new HashMap<String, Datapoint>();

            for (Iterator<String> it = source.sensorNameIterator(company, startMs, endMs); it.hasNext(); ) {
                String sensor = it.next();
                String[] names = sensor.split("!");
                String environment = names[0];
                String topic = names[2];
                String property = names[3];

                String topicAggregateSensorName = environment + "!" + topic + "!" + property;
                Datapoint datapoint = datapointMap.get(topicAggregateSensorName);
                if (datapoint == null) {
                    datapoint = new Datapoint();
                    datapointMap.put(topicAggregateSensorName, datapoint);
                }

                Iterator<HCounterColumn<UUID>> iterator = source.dataPointIterator(company, sensor, startMs, endMs);

                for (; iterator.hasNext(); ) {
                    HCounterColumn<UUID> columns = iterator.next();
                    long value = columns.getValue();
                    datapoint.map.put(sensor, value);
                }
            }

            for (Map.Entry<String, Datapoint> datapointEntry : datapointMap.entrySet()) {
                String sensorname = datapointEntry.getKey();
                Datapoint datapoint = datapointEntry.getValue();
                long total = 0;
                for (Long value : datapoint.map.values()) {
                    total += value;
                }

                target.insert(company, sensorname, endMs, total);
            }


        } catch (Throwable e) {
            logger.warn("Failed to do a rollup", e);
        }
    }

    static class Datapoint {
        Map<String, Long> map = new HashMap<String, Long>();
    }
}
