
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

/**
 * SnapshotManager is an object that allows you to manage (create and cleanup)
 * snapshots of a snapshottable directory in HDFS.
 *
 * You provide a frequency and retention parameter, which is used to
 * automatically take new snapshots and clean up outdated ones.
 */
public class SnapshotManager {
    private static final Log LOG = LogFactory.getLog(SnapshotManager.class);

    private Path directory; // Root directory to snapshot
    private Integer frequency; // Frequency to perform snapshots in minutes
    private Integer retention; // Number of snapshots to retain
    private String label; // Custom human readable label for snapshots

    private DistributedFileSystem filesystem;
    private SnapshottableDirectoryStatus directoryStatus;

    public SnapshotManager(DistributedFileSystem fs, Path path,
                           Integer frequency, Integer retention, String label)
                                   throws IOException, Exception {
        this.directory = path;
        this.frequency = frequency;
        this.retention = retention;
        this.label = label;
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
        if (latestSnapshot == null) {
            return true;
        }

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
        if (snapshots.size() == 0) {
            return null;
        }

        return snapshots.get(0);
    }

    private List<Snapshot> listSnapshots(boolean onlyOutdated) throws Exception {
        Path snapshotsDir = new Path(this.directory + "/.snapshot/hadoopit-" +
                                     this.frequency + "-*");
        ArrayList<Snapshot> snapshots = new ArrayList<Snapshot>();

        FileStatus[] files = null;
        try {
            files = this.filesystem.globStatus(snapshotsDir);
        } catch (FileNotFoundException e) { }

        if (files != null) {
            for (FileStatus status : files) {
                if (status.isDirectory()) {
                    Snapshot snapshot = new Snapshot(this.directoryStatus, status);
                    snapshots.add(snapshot);

                    LOG.info("Found snapshot " + snapshot);
                }
            }
        }

        // Sort the snapshots to ensure they are in chronological order
        Collections.sort(snapshots, new SnapshotComparator());
        Collections.reverse(snapshots);

        if (onlyOutdated) {
            Integer retainedSnapshots = 0;
            Iterator<Snapshot> iterator = snapshots.iterator();
            while (iterator.hasNext()) {
                iterator.next();
                retainedSnapshots++;

                if (retainedSnapshots <= this.retention) {
                    iterator.remove();
                }
            }
        }

        return snapshots;
    }

    public Path takeSnapshot() throws Exception {
        if (needToTakeSnapshot()) {
            DateTimeFormatter formatter = DateTimeFormat.forPattern(Snapshot.DATE_FORMAT);

            String label = "";
            if (this.label != null) {
                label = "-" + this.label;
            }

            String snapshotName = "hadoopit-" + this.frequency +
                                  "-" + formatter.print(DateTime.now()) + label;

            LOG.info("Creating snapshot with name " + snapshotName + " for path " + this.directory);
            Path snapshot = this.filesystem.createSnapshot(this.directory, snapshotName);

            if (snapshot != null) {
                return snapshot;
            }
        }

        return null;
    }

    public Integer cleanupOutdatedSnapshots() throws Exception {
        if (this.retention == 0) {
            return 0;
        }

        Integer snapshotsRemoved = 0;
        List<Snapshot> outdatedSnapshots = listOutdatedSnapshots();

        for (Snapshot s : outdatedSnapshots) {
            LOG.info("Deleting snapshot " + s.getPath());
            this.filesystem.deleteSnapshot(this.directory, s.getName());
            snapshotsRemoved++;
        }

        return snapshotsRemoved;
    }
}
