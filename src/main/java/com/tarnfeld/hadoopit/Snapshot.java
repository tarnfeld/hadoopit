
package com.tarnfeld.hadoopit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * Snapshot is an object that represents a single snapshot of a snapshottable
 * directory in HDFS.
 */
public class Snapshot {

    private SnapshottableDirectoryStatus snapshottableDir;
    private FileStatus directoryStatus;

    private String name;
    private DateTime created;
    private Integer frequency;
    private String label;

    public static String DATE_FORMAT = "yyyy.MM.dd.H.m.s.SSS";

    public Snapshot(SnapshottableDirectoryStatus dir, FileStatus status)
            throws Exception {
        this.snapshottableDir = dir;
        this.directoryStatus = status;

        parseStatus();
    }

    public SnapshottableDirectoryStatus getSnapshottableDirectoryStatus() {
        return this.snapshottableDir;
    }

    public FileStatus getDirectoryStatus() {
        return this.directoryStatus;
    }

    public String getName() {
        return this.name;
    }

    public DateTime getCreatedTime() {
        return this.created;
    }

    public Path getPath() {
        return this.directoryStatus.getPath();
    }

    public Integer getSnapshotFrequency() {
        return this.frequency;
    }

    public String getLabel() {
        return this.label;
    }

    @Override
    public String toString() {
        return this.snapshottableDir.getFullPath() + "[" + this.created + "]";
    }

    private void parseStatus() throws Exception {
        this.name = this.directoryStatus.getPath().getName();

        DateTimeFormatter formatter = DateTimeFormat.forPattern(Snapshot.DATE_FORMAT);

        String[] parts = this.name.split("-");
        if (parts.length < 3) {
            throw new Exception("Expected at least three parts from the snapshot name");
        }

        this.frequency = new Integer(parts[1]);
        if (parts.length > 3) {
            this.label = parts[3];
        }

        String timestamp = parts[2];
        this.created = formatter.parseDateTime(timestamp);

        if (this.created == null) {
            throw new Exception("Failed to parse date from snapshot " + this.name);
        }
    }
}
