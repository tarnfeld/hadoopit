
package com.tarnfeld.hadoopit;

import java.util.List;

import junit.extensions.TestSetup;
import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;

public class TestSnapshotManager extends TestCase {

    private static MiniDFSCluster cluster;

    public static Test suite() {
        TestSetup setup = new TestSetup(new TestSuite(TestSnapshotManager.class)) {
            @SuppressWarnings("deprecation")
            @Override
            protected void setUp() throws Exception {
                Configuration conf = new Configuration();
                cluster = new MiniDFSCluster(conf, 1, true, null);
            }
            @Override
            protected void tearDown() throws Exception {
                if (cluster != null) { cluster.shutdown(); }
            }
        };
        return setup;
    }

    public void testTakeSnapshot() throws Exception {
        DistributedFileSystem fs = cluster.getFileSystem();
        Path dir = new Path("/a");

        fs.mkdir(dir, null);
        fs.allowSnapshot(dir);

        SnapshotManager manager = new SnapshotManager(fs, dir, 1, 1, null);

        assertEquals(manager.listAllSnapshots().size(), 0);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);

        assertEquals(manager.needToTakeSnapshot(), true);
        assertNotNull(manager.takeSnapshot());

        assertEquals(manager.listAllSnapshots().size(), 1);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);
    }

    public void testTakeMultipleSnapshots() throws Exception {
        DistributedFileSystem fs = cluster.getFileSystem();
        Path dir = new Path("/b");

        fs.mkdir(dir, null);
        fs.allowSnapshot(dir);

        SnapshotManager manager = new SnapshotManager(fs, dir, 1, 2, null);

        assertEquals(manager.listAllSnapshots().size(), 0);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);

        assertEquals(manager.needToTakeSnapshot(), true);
        assertNotNull(manager.takeSnapshot());
        assertEquals(manager.needToTakeSnapshot(), false);
        assertNull(manager.takeSnapshot());

        assertEquals(manager.listAllSnapshots().size(), 1);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);

        // Sleep for 1 minute so we can take another snapshot
        Thread.sleep(1000 * 61);

        assertEquals(manager.listAllSnapshots().size(), 1);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);

        assertEquals(manager.needToTakeSnapshot(), true);
        assertNotNull(manager.takeSnapshot());
        assertEquals(manager.needToTakeSnapshot(), false);
        assertNull(manager.takeSnapshot());

        assertEquals(manager.listAllSnapshots().size(), 2);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);
    }

    public void testTakeMultipleSnapshotsWithOudated() throws Exception {
        DistributedFileSystem fs = cluster.getFileSystem();
        Path dir = new Path("/c");

        fs.mkdir(dir, null);
        fs.allowSnapshot(dir);

        SnapshotManager manager = new SnapshotManager(fs, dir, 1, 1, null);

        assertEquals(manager.listAllSnapshots().size(), 0);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);

        assertEquals(manager.needToTakeSnapshot(), true);
        assertNotNull(manager.takeSnapshot());
        assertEquals(manager.needToTakeSnapshot(), false);
        assertNull(manager.takeSnapshot());

        assertEquals(manager.listAllSnapshots().size(), 1);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);

        // Sleep for 1 minute so we can take another snapshot
        Thread.sleep(1000 * 61);

        assertEquals(manager.listAllSnapshots().size(), 1);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);

        assertEquals(manager.needToTakeSnapshot(), true);
        assertNotNull(manager.takeSnapshot());
        assertEquals(manager.needToTakeSnapshot(), false);
        assertNull(manager.takeSnapshot());

        assertEquals(manager.listAllSnapshots().size(), 2);
        assertEquals(manager.listOutdatedSnapshots().size(), 1);

        // Sleep for 1 minute so we can take another snapshot
        Thread.sleep(1000 * 61);

        assertEquals(manager.needToTakeSnapshot(), true);
        assertNotNull(manager.takeSnapshot());
        assertEquals(manager.needToTakeSnapshot(), false);
        assertNull(manager.takeSnapshot());

        assertEquals(manager.listAllSnapshots().size(), 3);
        assertEquals(manager.listOutdatedSnapshots().size(), 2);
    }

    public void testCleanupOutdatedSnapshots() throws Exception {
        DistributedFileSystem fs = cluster.getFileSystem();
        Path dir = new Path("/d");

        fs.mkdir(dir, null);
        fs.allowSnapshot(dir);

        SnapshotManager manager = new SnapshotManager(fs, dir, 1, 1, null);

        // Take the first snapshot
        manager.takeSnapshot();
        Thread.sleep(1000 * 61);

        // Take the second snapshot
        manager.takeSnapshot();
        Thread.sleep(1000 * 61);

        // Cleanup the outdated snapshot
        assertEquals(manager.listAllSnapshots().size(), 2);
        assertEquals(manager.listOutdatedSnapshots().size(), 1);

        assertEquals(manager.cleanupOutdatedSnapshots(), (Integer) 1);

        assertEquals(manager.listAllSnapshots().size(), 1);
        assertEquals(manager.listOutdatedSnapshots().size(), 0);
    }

    public void testCleanupOutdatedSnapshotsCorrectPaths() throws Exception {
        DistributedFileSystem fs = cluster.getFileSystem();
        Path dir = new Path("/e");

        fs.mkdir(dir, null);
        fs.allowSnapshot(dir);

        SnapshotManager manager = new SnapshotManager(fs, dir, 1, 1, null);

        // Take the first snapshot
        Path first = Path.getPathWithoutSchemeAndAuthority(manager.takeSnapshot());
        Thread.sleep(1000 * 61);

        // Take the second snapshot
        Path second = Path.getPathWithoutSchemeAndAuthority(manager.takeSnapshot());
        Thread.sleep(1000 * 61);

        List<Snapshot> snapshots;

        snapshots = manager.listAllSnapshots();
        assertEquals(snapshots.size(), 2);
        assertEquals(Path.getPathWithoutSchemeAndAuthority(snapshots.get(0).getPath()), first);
        assertEquals(Path.getPathWithoutSchemeAndAuthority(snapshots.get(1).getPath()), second);

        snapshots = manager.listOutdatedSnapshots();
        assertEquals(Path.getPathWithoutSchemeAndAuthority(snapshots.get(0).getPath()), first);
        assertEquals(snapshots.size(), 1);

        assertEquals(manager.cleanupOutdatedSnapshots(), (Integer) 1);

        snapshots = manager.listAllSnapshots();
        assertEquals(snapshots.size(), 1);
        assertEquals(Path.getPathWithoutSchemeAndAuthority(snapshots.get(0).getPath()), second);

        assertEquals(manager.listOutdatedSnapshots().size(), 0);
    }
}
