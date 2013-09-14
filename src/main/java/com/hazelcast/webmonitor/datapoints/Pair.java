package com.hazelcast.webmonitor.datapoints;

public class Pair {
    private final Datapoint left;
    private final Datapoint right;

    public Pair(Datapoint left, Datapoint right) {
        this.left = left;
        this.right = right;
    }

    public Datapoint getLeft() {
        return left;
    }

    public Datapoint getRight() {
        return right;
    }
}
