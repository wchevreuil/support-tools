package com.cloudera.support.hbase;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.master.snapshot.SnapshotHFileCleaner;

/**
 * This program allows to inspect hfiles on specified path,
 * checking if the given hfiles are still referenced by any existing snapshot.
 * 
 * The main goal is to be used as an alternative tool to HBase's own 
 * archive cleaning feature, mainly on situations where archive folder
 * has grown too large, and HMaster's cleaner thread is taking too long
 * to reclaim HDFS space, whilst using this tool, it's possible to specify
 * specific subdirs within archive directory, for example, older dirs or those
 * related to a recently deleted snapshot, which are more likely to have 
 * unreferenced files.
 * 
 * It traverses the specified path recursively. 
 * It should be only passed subdirectories of hbase archive folder. 
 * It currently does not not perform any validation on that, passing paths for 
 * hbase tables (i.e. "/hbase/data/default/table_name") may lead to 
 * unpredicted results and has not been tested. Please don't do that.
 * 
 * Usage:
 *
 * com.cloudera.support.hbase.ArchiveFileSnapshotReference ARCHIVE_SUBDIR [delete]
 * 
 * The only required parameter is the archive subdir. In this mode, 
 * it will go through the informed directry structure, 
 * checking hfiles that can be deleted. At the end a summary of how much files
 * would be deleted and hdfs space would be released is printed.
 * 
 * Passing optional "delete" flag as second parameter would cause the unreferenced
 * files to be deleted, releasing hdfs space.
 */
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
    long totalBytesReleaseable = 0;

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
