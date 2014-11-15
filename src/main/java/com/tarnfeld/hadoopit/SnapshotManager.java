
package com.tarnfeld.hadoopit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;


public class SnapshotManager {
    private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

    private Path directory; // Root directory to snapshot
    private Integer frequency; // Frequency to perform snapshots in minutes
    private Integer retention; // Number of snapshots to retain

    private DistributedFileSystem filesystem;
    private SnapshottableDirectoryStatus directoryStatus;

    public SnapshotManager(DistributedFileSystem fs, String snapshotDirectory,
                           Integer frequency, Integer retention) throws IOException, Exception {
        this.directory = new Path(snapshotDirectory);
        this.frequency = frequency;
        this.retention = retention;
        this.filesystem = fs;

        for (SnapshottableDirectoryStatus dir : fs.getSnapshottableDirListing()) {
            if (dir.getFullPath().equals(this.directory)) {
                this.directoryStatus = dir;
                break;
            }
        }

        if (this.directoryStatus == null) {
            throw new Exception("The directory " + this.directory + " is not snapshottable");
        }
    }

    public boolean needToTakeSnapshot() {
        return false;
    }

    public List<Snapshot> listSnapshots() throws IOException {
        return listSnapshots(false);
    }

    public List<Snapshot> listSnapshots(boolean outdated) throws IOException {
        Path snapshotsDir = new Path(this.directory + "/.snapshot");
        ArrayList<Snapshot> snapshots = new ArrayList<Snapshot>();

        FileStatus[] files = null;
        try {
            files = this.filesystem.listStatus(snapshotsDir);
        } catch (FileNotFoundException e) { }

        if (files != null) {
            for (FileStatus status : files) {
                if (status.isDirectory()) {
                    Snapshot snapshot = new Snapshot(this.directoryStatus, status);
                    snapshots.add(snapshot);
                }
            }
        }

        return snapshots;
    }

    public boolean takeSnapshot() {
        if (!needToTakeSnapshot()) {
            return false;
        }

        // TODO(tarnfeld): Perform the filesystem snapshot

        return true;
    }

    public Integer cleanupOutdatedSnapshots() {
        // TODO(tarnfeld): Cleanup outdated snapshots

        return 0;
    }
}
