package com.tarnfeld.hadoopit;

import java.util.Comparator;

/**
 * SnapshotComparator is a comparator that can be used to compare snapshots
 * by the time they were created.
 */
public class SnapshotComparator implements Comparator<Snapshot> {
    @Override
    public int compare(Snapshot s1, Snapshot s2) {
        return s1.getCreatedTime().compareTo(s2.getCreatedTime());
    }
}