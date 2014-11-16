
package com.tarnfeld.hadoopit;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

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

    public boolean needToTakeSnapshot() throws Exception {
        Snapshot latestSnapshot = getLatestSnapshot();
        DateTime snapshotDue = latestSnapshot.getCreatedTime().plusMinutes(this.frequency);

        return snapshotDue.isBeforeNow();
    }

    public List<Snapshot> listAllSnapshots() throws Exception {
        return listSnapshots(false);
    }

    public List<Snapshot> listOutdatedSnapshots() throws Exception {
        return listSnapshots(true);
    }

    public Snapshot getLatestSnapshot() throws Exception {
        List<Snapshot> snapshots = listAllSnapshots();
        return snapshots.get(snapshots.size() - 1);
    }

    private List<Snapshot> listSnapshots(boolean onlyOutdated) throws Exception {
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

        // Sort the snapshots to ensure they are in chronological order
        Collections.sort(snapshots, new SnapshotComparator());

        Integer retainedSnapshots = 0;
        for (Iterator<Snapshot> iterator = snapshots.iterator(); iterator.hasNext();) {
            retainedSnapshots++;

            if (onlyOutdated && retainedSnapshots <=  this.retention) {
                iterator.remove();
            }
        }

        return snapshots;
    }

    public boolean takeSnapshot() throws Exception {
        if (needToTakeSnapshot()) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(Snapshot.DATE_FORMAT);
            String snapshotName = "s" + formatter.print(DateTime.now());

            LOG.info("Creating snapshot with name " + snapshotName + " for path " + this.directory);
            Path snapshot = this.filesystem.createSnapshot(this.directory, snapshotName);

            if (snapshot != null) {
                return true;
            }
        }

        return false;
    }

    public Integer cleanupOutdatedSnapshots() throws Exception {
        Integer snapshotsRemoved = 0;
        List<Snapshot> outdatedSnapshots = listOutdatedSnapshots();

        for (Snapshot s : outdatedSnapshots) {
            this.filesystem.deleteSnapshot(this.directory, s.getName());
            snapshotsRemoved++;
        }

        return snapshotsRemoved;
    }
}
