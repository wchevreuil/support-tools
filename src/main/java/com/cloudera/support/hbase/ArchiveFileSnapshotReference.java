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

    for (FileStatus f : references.fileCleaner.getDeletableFiles(filesToCheck)) {

      System.out.println("file " + f.toString() + " will be deleted now");

      references.fs.delete(f.getPath(), false);

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
