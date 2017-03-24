package com.cloudera.support.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner;

public class ArchiveFileSnapshotReference {

  private SnapshotHFileCleaner fileCleaner = new SnapshotHFileCleaner();

  private FileSystem fs;

  public static void main(String[] args) throws Exception {

    ArchiveFileSnapshotReference references = new ArchiveFileSnapshotReference();

    references.fs = FileSystem.get(new Configuration());

    references.fileCleaner.setConf(HBaseConfiguration.create());

    List<FileStatus> filesToCheck = new ArrayList<FileStatus>();

    references.checkArchiveFolder(new Path(args[0]), filesToCheck);

    int countDeletable = 0;
    int totalBytesReleaseable = 0;

    for (FileStatus f : references.fileCleaner.getDeletableFiles(filesToCheck)) {

      countDeletable++;

      totalBytesReleaseable += f.getLen();

      System.out.println(
        "file " + f.getPath() + " is deletable, and would release " + f.getLen() + " bytes");

      if (args.length == 2 && args[1].equals("delete")) {
        System.out.println("file " + f.getPath() + " will be deleted now");
        references.fs.delete(f.getPath(), false);
        System.out.println("deleted file " + f.getPath() + " of size " + f.getLen() + " bytes");
      }

    }

    if (countDeletable == 0) {
      System.out.println("Finished iterating. No file under " + args[0] + " is deletable");
    } else {
      System.out.println("Total deletable files under " + args[0] + ": " + countDeletable);
      if (args.length == 2 && args[1].equals("delete")) {
        System.out
            .println("Total space released with deletion: " + totalBytesReleaseable + " bytes");
      } else {
        System.out.println("Space to be released: " + totalBytesReleaseable + " bytes");
      }
    }

    references.fileCleaner.stop("closing the utility program");

    references.fs.close();

  }

  public void checkArchiveFolder(Path root, List<FileStatus> files) throws Exception {

    for (FileStatus f : fs.listStatus(root)) {

      if (f.isDirectory()) {

        checkArchiveFolder(f.getPath(), files);

      } else {

        files.add(f);

      }

    }
  }

}
