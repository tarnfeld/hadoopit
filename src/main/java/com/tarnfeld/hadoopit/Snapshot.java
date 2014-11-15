
package com.tarnfeld.hadoopit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class Snapshot {
    private static final Log LOG = LogFactory.getLog(Snapshot.class);

    private SnapshottableDirectoryStatus snapshottableDir;
    private FileStatus directoryStatus;

    public Snapshot(SnapshottableDirectoryStatus dir, FileStatus status) {
        this.snapshottableDir = dir;
        this.directoryStatus = status;
    }

    public SnapshottableDirectoryStatus getSnapshottableDirectoryStatus() {
        return this.snapshottableDir;
    }

    public String getName() {
        return this.directoryStatus.getPath().getName();
    }

    public FileStatus getSnapshotStatus() {
        return this.directoryStatus;
    }

    public DateTime getSnapshotTime() throws Exception {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyyMMdd-Hms.SSS");

        String name = this.getName();
        DateTime dateTime = formatter.parseDateTime(name.substring(1, name.length()));

        if (dateTime == null) {
            throw new Exception("Failed to parse date from snapshot " + name);
        }

        return dateTime;
    }

    @Override
    public String toString() {
        String dateString = null;

        try {
            dateString = getSnapshotTime().toString();
        } catch (Exception e) {
            LOG.error("Caught exception getting the snapshot time " + e);
        }

        return this.directoryStatus.getPath().getParent() + "[" + dateString + "]";
    }
}
