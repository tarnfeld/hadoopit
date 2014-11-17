hadoopit
========

Hadoopit is a command line tool that helps manage automated Point-In-Time snapshots of data in a HDFS cluster. You can use Hadoopit to automate backups to help prevent data loss due to accidents or human error. Different frequencies of snapshots can be used, and retained for different periods. For example;

- Hourly snapshots retained for 24 hours (`--snapshot-freq 60 --snapshot-retention 24`)
- Daily snapshots retained for 1 week (`--snapshot-freq 1440 --snapshot-retention 7`)
- Weekly snapshots retained for 1 month (`--snapshot-freq 10080 --snapshot-retention 4`)
- Monthly snapshots retained for 3 months (`--snapshot-freq 43200 --snapshot-retention 3`)

The `--snapshot-freq` argument is required and specifies how often to take a snapshot, in _minutes_. The `--snapshot-retention` argument specifies how many snapshots to retain, use `0` to retain infinite snapshots.

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
      --dry-run
      --snapshot-label(-l) LABEL

Example (Daily snapshots kept for a week);
  $ hadoop com.tarnfeld.hadoopit.CommandLineTool -d /data -f 1440 -r 7 -l daily
```
