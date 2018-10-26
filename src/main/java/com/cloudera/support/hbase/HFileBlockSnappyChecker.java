package com.cloudera.support.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.*;
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

  boolean printKeysOnly;

  public static void main(String[] args) throws Exception {

    Path filePath = new Path(args[0]);

    HFileBlockSnappyChecker checker = new HFileBlockSnappyChecker();

    checker.printKeysOnly = args.length == 2 ? args[1].equals("-keysOnly") :
        false;

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

    StringBuilder failedKeys = new StringBuilder();

    failedKeys.append("-------> key ranges for failed blocks:");

    boolean lastBlockFailed = false;

    String lastKey = null;

    for(BlockInfo info : blocksInfo) {

      System.out.println(">>>>> block: " + info.blockIndex + ", startkey: " +
          info
          .startKey + ", endkey:" + info.endKey);
      try {

        HFileScanner scanner = reader.getScanner(false, false, false);

        boolean scanResult = scanner.seekBefore(KeyValueUtil.createFirstOnRow(Bytes.toBytesBinary(info.startKey.split
            ("/")[0])));

        System.out.println(scanResult);

        if (!scanResult) {
          scanner.seekTo();
        }

        int cellsCount = 0;

        String currentKey = null;

        if(lastKey == null || lastKey.compareTo(info.startKey.split
            ("/")[0]) < 0) {

          do {

            cell = scanner.getKeyValue();

            cellKey = scanner.getKeyString();

            System.out.println(cellKey);

            currentKey = cell.toString().split("/")[0];

            System.out.println("---> current key: " + currentKey);

            if (printKeysOnly) {

              if (!currentKey.equals(lastKey)) {
                System.out.println(currentKey);
                lastKey = currentKey;
                if (lastBlockFailed) {
                  failedKeys.append(" -> " + lastKey);
                  lastBlockFailed = false;
                }
              }
            }

            cellsCount++;

          } while (scanner.next());

        }

        System.out.println("Cells read on this block: " + cellsCount);

        if(!printKeysOnly) {
          System.out.println("--------------");
          System.out.println("Checked block " + info.blockIndex + ".");
          System.out.println("Block start key: " + info.startKey);
          System.out.println("Block offset: " + info.offset);
          System.out.println("Block size: " + info.dataSize);
          System.out.println("Latest KV in this block: " +
              cell + ":" + Bytes.toStringBinary(cell.getValue()));
        }
        successBlocks++;

      } catch (Throwable t) {

        System.out.println("error in this block");
        if(!printKeysOnly) {

          System.out.println("--------------");
          System.out.println("Error checking block " + info.blockIndex + ".");
          System.out.println("Block start key: " + info.startKey);
          System.out.println("Block offset: " + info.offset);
          System.out.println("Block size: " + info.dataSize);
          System.out.println("Latest KV in this block: " +
              ( cell != null ? cell + ":" + Bytes.toStringBinary(cell
                  .getValue()) : ""));
          System.out.println(t + " : " + t.getMessage());
          t.printStackTrace();

        } else {

          if(!lastBlockFailed) {

            failedKeys.append("\n" + info.startKey.split
                ("/")[0]);


            lastBlockFailed = true;

          }

        }


        failedBlocks++;

      } /*finally {

        reader.close();

      }*/
    }

    if(printKeysOnly){
      System.out.println(failedKeys);
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
