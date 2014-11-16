hadoopit
========

Hadoopit is a command line tool that helps manage automated Point-In-Time snapshots of a data in a HDFS cluster.

```
Usage: hadoop com.tarnfeld.hadoopit.CommandLineTool [-hdfr]

Hadoopit is a CLI tool for automating HDFS directory snapshots.

You should schedule (e.g with cron) Hadoopit for each frequency and
level of retention you desire. You can specify a label for each type
of snapshot you want to retain, which can help with human readability.

Required Options:
      --snapshot-dir(-d) DIRECTORY
      --snapshot-freq(-f) FREQUENCY
      --snapshot-retention(-r) RETENTION

Optional Options:
      --help(-h)
      --snapshot-label(-l) LABEL

Example (Daily snapshots kept for a week);
  $ hadoop com.tarnfeld.hadoopit.CommandLineTool -d /data -f 1440 -r 7 -l daily
```
