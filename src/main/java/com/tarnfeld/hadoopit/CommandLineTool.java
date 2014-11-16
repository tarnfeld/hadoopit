
package com.tarnfeld.hadoopit;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

/**
 * CommandLineTool is a thing for things
 */
public class CommandLineTool {
    private static final Log LOG = LogFactory.getLog(CommandLineTool.class);

    @Parameter
    private List<String> parameters = new ArrayList<String>();

    @Parameter(names={"-h", "--help"}, help=true)
    private boolean help;

    @Parameter(names="--dry-run",
               description="Don't modify snapshots, just print out details")
    private boolean dry = false;

    @Parameter(names={"-d", "--snapshot-dir"},
               description="Directory to in HDFS to snapshot",
               required=true)
    private String directory;

    @Parameter(names={"-f", "--snapshot-freq"},
               description="Frequency to perform snapshots (minutes)",
               required=true)
    private Integer frequency;

    @Parameter(names={"-r", "--snapshot-retention"},
               description="Number of historic snapshot to retain",
               required=true)
    private Integer retention;

    public int run() throws Exception {
        if (this.help) {
            // TODO(tarnfeld): Print out help and usage
            return 1;
        }

        // Get the HDFS filesystem
        FileSystem filesystem = FileSystem.get(getHadoopConfiguration());
        if (!(filesystem instanceof DistributedFileSystem)) {
            LOG.error("Cannot create snapshots of filesystem that's not HDFS");
            return 1;
        }

        SnapshotManager manager = new SnapshotManager(
            (DistributedFileSystem) filesystem,
            this.directory,
            this.frequency,
            this.retention
        );

        if (this.dry) {
            if (manager.needToTakeSnapshot()) {
                LOG.info("Would take snapshot of " + this.directory);
            }

            for (Snapshot s : manager.listOutdatedSnapshots()) {
                LOG.info("Would clean out old snapshot: " + s.toString());
            }
        } else {
            if (!manager.takeSnapshot()) {
                LOG.info("Skipped creating a snapshot of " + this.directory);
            } else {
                LOG.info("Created snapshot of " + this.directory);
            }

            Integer cleaned = manager.cleanupOutdatedSnapshots();
            LOG.info("Cleaned up " + cleaned + " outdated snapshots");
        }

        return 0;
    }

    private Configuration getHadoopConfiguration() throws Exception {
        String hadoopConfDir = System.getenv("HADOOP_CONF_DIR");
        if (hadoopConfDir == null) {
            throw new Exception("The HADOOP_CONF_DIR variable is required");
        }

        while (hadoopConfDir.endsWith("/")) {
            hadoopConfDir = hadoopConfDir.substring(0, hadoopConfDir.length() - 1);
        }

        Configuration conf = new Configuration();
        conf.addResource(new Path(hadoopConfDir + "/core-site.xml"));
        conf.addResource(new Path(hadoopConfDir + "/hdfs-site.xml"));

        return conf;
    }

    public static void main(String[] args) throws Exception {
        CommandLineTool app = new CommandLineTool();

        // Parse command line arguments
        new JCommander(app, args);

        // Run the application
        int exitCode = app.run();
        System.exit(exitCode);
    }
}
