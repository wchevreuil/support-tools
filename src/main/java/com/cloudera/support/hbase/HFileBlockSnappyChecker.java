package com.cloudera.support.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.io.hfile.CacheConfig;
import org.apache.hadoop.hbase.io.hfile.HFile;
import org.apache.hadoop.hbase.io.hfile.HFileBlockIndex;
import org.apache.hadoop.hbase.io.hfile.HFileScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.FSUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Created by wchevreuil on 17/05/2018.
 */
public class HFileBlockSnappyChecker {

  public static void main(String[] args) throws Exception {

    Path filePath = new Path(args[0]);

    HFileBlockSnappyChecker checker = new HFileBlockSnappyChecker();

    checker.checkBlocksIntegrity(checker.getBlocksStartKeys(filePath), filePath);

  }

  public List<BlockInfo> getBlocksStartKeys(Path hFile) throws Exception {

    List<BlockInfo> blocksInfo = new ArrayList<>();

    Configuration config = HBaseConfiguration.create();

    config.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);

    FileSystem fs = hFile.getFileSystem(config);

    HFile.Reader reader = HFile.createReader(fs, hFile, new CacheConfig(config),
        config);

    Map<byte[], byte[]> fileInfo = reader.loadFileInfo();

    HFileBlockIndex.BlockIndexReader index = reader.getDataBlockIndexReader();

    for(int i=0; i<index.getRootBlockCount(); i++){
      BlockInfo info = new BlockInfo();
      info.startKey = KeyValue.keyToString(index.getRootBlockKey(i));
      info.offset = index.getRootBlockOffset(i);
      info.dataSize = index.getRootBlockDataSize(i);
      info.blockIndex = i;
      if ( i < index.getRootBlockCount()-1){
        info.endKey = KeyValue.keyToString(index.getRootBlockKey(i+1));
      }
      blocksInfo.add(info);
    }

    reader.close();

    return blocksInfo;

  }

  public void checkBlocksIntegrity(List<BlockInfo> blocksInfo, Path hFile) throws Exception {

    Configuration config = HBaseConfiguration.create();

    config.setFloat(HConstants.HFILE_BLOCK_CACHE_SIZE_KEY, 0);

    FSUtils.setFsDefault(config, FSUtils.getRootDir(config));

    FileSystem fs = hFile.getFileSystem(config);

    HFile.Reader reader = HFile.createReader(fs, hFile, new CacheConfig(config),
        config);

    Map<byte[], byte[]> fileInfo = reader.loadFileInfo();

    int successBlocks = 0;

    int failedBlocks = 0;

    Cell cell = null;

    String cellKey = null;

    for(BlockInfo info : blocksInfo) {

      try {

        HFileScanner scanner = reader.getScanner(false, false, false);

        int scanResult = scanner.seekTo(Bytes.toBytesBinary(info.startKey.split
            ("//")[0]));

        if (scanResult == -1) {
          scanner.seekTo();
        }

        do {

          cell = scanner.getKeyValue();

          cellKey = scanner.getKeyString();

        } while (scanner.next() && !cellKey.equals(info.endKey));

        System.out.println("--------------");
        System.out.println("Checked block " + info.blockIndex + ".");
        System.out.println("Block start key: " + info.startKey);
        System.out.println("Block offset: " + info.offset);
        System.out.println("Block size: " + info.dataSize);
        System.out.println("Latest KV in this block: " +
            cell);
        successBlocks++;

      } catch (Throwable t) {

        System.out.println("--------------");
        System.out.println("Error checking block " + info.blockIndex + ".");
        System.out.println("Block start key: " + info.startKey);
        System.out.println("Block offset: " + info.offset);
        System.out.println("Block size: " + info.dataSize);
        System.out.println("Latest KV before the error: " + cell);
        System.out.println(t + " : " + t.getMessage());
        t.printStackTrace();
        failedBlocks++;

      }
    }

    System.out.println("--------");
    System.out.println("Blocks readable: " + successBlocks);
    System.out.println("Blocks corrupt: " + failedBlocks);

  }


}

class BlockInfo {

  String startKey;
  String endKey;
  long offset;
  long dataSize;
  int blockIndex;
}
