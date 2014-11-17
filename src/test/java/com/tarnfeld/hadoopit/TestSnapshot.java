
package com.tarnfeld.hadoopit;

import junit.framework.TestCase;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.SnapshottableDirectoryStatus;
import org.joda.time.DateTime;

public class TestSnapshot extends TestCase {

    public void testSnapshotWithLabel() throws Exception {
        Path path = new Path("/foo/bar/hadoopit-10-2014.01.01.01.01.01.000-test");
        SnapshottableDirectoryStatus dir = getDirectoryStatus(path.getParent());
        FileStatus status = getFileStatus(path);

        Snapshot snapshot = new Snapshot(dir, status);

        assertEquals(snapshot.getSnapshottableDirectoryStatus(), dir);
        assertEquals(snapshot.getDirectoryStatus(), status);
        assertEquals(snapshot.getPath(), path);

        assertEquals(snapshot.getName(), "hadoopit-10-2014.01.01.01.01.01.000-test");
        assertEquals(snapshot.getCreatedTime(), new DateTime(2014, 01, 01, 01, 01, 01));
        assertEquals(snapshot.getSnapshotFrequency(), (Integer) 10);
        assertEquals(snapshot.toString(), "/foo/bar[2014-01-01T01:01:01.000Z]");
        assertEquals(snapshot.getLabel(), "test");
    }

    public void testSnapshotWithoutLabel() throws Exception {
        Path path = new Path("/foo/bar/hadoopit-10-2014.01.01.01.01.01.000");
        SnapshottableDirectoryStatus dir = getDirectoryStatus(path.getParent());
        FileStatus status = getFileStatus(path);

        Snapshot snapshot = new Snapshot(dir, status);

        assertEquals(snapshot.getSnapshottableDirectoryStatus(), dir);
        assertEquals(snapshot.getDirectoryStatus(), status);
        assertEquals(snapshot.getPath(), path);

        assertEquals(snapshot.getName(), "hadoopit-10-2014.01.01.01.01.01.000");
        assertEquals(snapshot.getCreatedTime(), new DateTime(2014, 01, 01, 01, 01, 01));
        assertEquals(snapshot.getSnapshotFrequency(), (Integer) 10);
        assertEquals(snapshot.toString(), "/foo/bar[2014-01-01T01:01:01.000Z]");
        assertEquals(snapshot.getLabel(), null);
    }

    private SnapshottableDirectoryStatus getDirectoryStatus(Path path) {
        return new SnapshottableDirectoryStatus(0, 0, null, null, null, path.toString().getBytes(), 0, 0, 1, 1, new byte[0]);
    }

    private FileStatus getFileStatus(Path path) {
        return new FileStatus(0, true, 1, 0, 0, 0, null, null, null, path);
    }
}
