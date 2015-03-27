
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
 * CommandLineTool for managing HDFS snapshots. Use `--help` to print usage
 * information to the command line.
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
               description="Frequency to perform snapshots (in minutes)",
               required=true)
    private Integer frequency;

    @Parameter(names={"-r", "--snapshot-retention"},
               description="Number of historic snapshots to retain",
               required=true)
    private Integer retention;

    @Parameter(names={"-l", "--label"},
               description="Label for this snapshot frequency")
    private String label;

    public int run() throws Exception {
        if (this.help) {
            System.err.println("Usage: hadoop com.tarnfeld.hadoopit.CommandLineTool [-hdfr]");
            System.err.println("\nHadoopit is a CLI tool for automating HDFS directory snapshots.\n");
            System.err.println("You should schedule (e.g with cron) Hadoopit for each frequency and\n" +
                               "level of retention you desire. You can specify a label for each type\n" +
                               "of snapshot you want to retain, which can help with human readability.");

            System.err.println("\nRequired Options:");
            System.err.println("      --snapshot-dir(-d) DIRECTORY" +
                               "\n      --snapshot-freq(-f) FREQUENCY" +
                               "\n      --snapshot-retention(-r) RETENTION");

            System.err.println("\nOptional Options:");
            System.err.println("      --help(-h)" +
                               "\n      --dry-run" +
                               "\n      --snapshot-label(-l) LABEL");

            System.err.println("\nExample (Snapshot /data every 24 hours and retain them for a week)");
            System.err.println("  $ hadoop com.tarnfeld.hadoopit.CommandLineTool -d /data -f 1440 -r 7 -l daily\n");

            return 1;
        }

        // Get the HDFS filesystem
        FileSystem filesystem = FileSystem.get(getHadoopConfiguration());
        if (!(filesystem instanceof DistributedFileSystem)) {
            LOG.error("Can't create snapshots from filesystem that's not HDFS");
            return 1;
        }

        SnapshotManager manager = new SnapshotManager(
            (DistributedFileSystem) filesystem,
            new Path(this.directory),
            this.frequency,
            this.retention,
            this.label
        );

        if (this.dry) {
            if (manager.needToTakeSnapshot()) {
                LOG.info("Would take snapshot of " + this.directory);
            }

            for (Snapshot s : manager.listOutdatedSnapshots()) {
                LOG.info("Would clean out old snapshot: " + s.toString());
            }
        } else {
            if (manager.takeSnapshot() != null) {
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
