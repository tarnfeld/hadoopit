
package com.tarnfeld.hadoopit;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class Snapshot {

    private SnapshottableDirectoryStatus snapshottableDir;
    private FileStatus directoryStatus;

    private String name;
    private DateTime created;

    public static String DATE_FORMAT = "yyyyMMdd-Hms.SSS";

    public Snapshot(SnapshottableDirectoryStatus dir, FileStatus status)
            throws Exception {
        this.snapshottableDir = dir;
        this.directoryStatus = status;

        parseStatus();
    }

    public SnapshottableDirectoryStatus getSnapshottableDirectoryStatus() {
        return this.snapshottableDir;
    }

    public String getName() {
        return this.name;
    }

    public FileStatus getSnapshotStatus() {
        return this.directoryStatus;
    }

    public DateTime getCreatedTime() {
        return this.created;
    }

    public Path getPath() {
        return this.directoryStatus.getPath();
    }

    @Override
    public String toString() {
        return this.directoryStatus.getPath().getParent() + "[" + this.created + "]";
    }

    private void parseStatus() throws Exception {
        this.name = this.directoryStatus.getPath().getName();

        DateTimeFormatter formatter = DateTimeFormat.forPattern(Snapshot.DATE_FORMAT);

        String name = this.name;
        this.created = formatter.parseDateTime(name.substring(1, name.length()));

        if (this.created == null) {
            throw new Exception("Failed to parse date from snapshot " + name);
        }
    }
}
