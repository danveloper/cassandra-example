package com.hazelcast.webmonitor.cassandra.datapoints;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;

public class DatapointUtils {

    /**
     * Finds all the datapoints that have the same timestamp.
     * <p/>
     * The returned datapoints will be in the same order as the left list.
     * <p/>
     * If no matching datapoints are found, the returned list is empty.
     *
     * @param left
     * @param right
     * @return
     */
    public static List<Pair> matchingTimestamps(List<Datapoint> left, List<Datapoint> right) {
        if (left.isEmpty() || right.isEmpty()) {
            return Collections.EMPTY_LIST;
        }

        LinkedHashMap<Long, Datapoint> x = new LinkedHashMap<Long, Datapoint>();

        for (Datapoint datapoint2 : right) {
            x.put(datapoint2.timestampMs, datapoint2);
        }

        List<Pair> result = new LinkedList<Pair>();
        for (Datapoint datapoint1 : left) {
            Datapoint datapoint2 = x.get(datapoint1.timestampMs);
            if (datapoint2 != null) {
                result.add(new Pair(datapoint1, datapoint2));
            }
        }

        return result;
    }

    private DatapointUtils() {
    }

    public static void print(List<Datapoint> datapoints) {
        print(null, datapoints);
    }

    public static void print(String description, List<Datapoint> datapoints) {
        if (description != null){
            System.out.println(description);
        }

        int k = 1;
        for (Datapoint datapoint : datapoints) {
            System.out.println(k + " " + datapoint);
            k++;
        }
    }
}
