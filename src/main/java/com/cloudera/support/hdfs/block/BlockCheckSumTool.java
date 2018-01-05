package com.cloudera.support.hdfs.block;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.apache.hadoop.hdfs.server.datanode.BlockMetadataHeader;
import org.apache.hadoop.util.DataChecksum;

public class BlockCheckSumTool {

  private static int HEADER_LEN = 7;

  public static void main(String[] args) throws Exception {

    FileInputStream metaStream = null, dataStream = null;

    FileChannel metaChannel = null, dataChannel = null;

    DataInputStream checksumStream = null;

    BlockMetadataHeader header;

    metaStream = new FileInputStream(args[0]);

    checksumStream = new DataInputStream(metaStream);

    header = BlockMetadataHeader.readHeader(checksumStream);

    metaChannel = metaStream.getChannel();

    metaChannel.position(HEADER_LEN);

    DataChecksum checksum = header.getChecksum();

    DataChecksum newChecksum = DataChecksum.newDataChecksum(DataChecksum.Type.CRC32C, 512);

    System.out.println("Checksum type: " + checksum.toString());

    System.out.println("Checksum size:" + checksum.getChecksumSize());

    System.out.println("Checksum bytes:" + checksum.getBytesPerChecksum());

    System.out.println("Checksum value: " + checksum.getValue());

    System.out.println(checksum.getHeader());

    ByteBuffer metaBuf, dataBuf, newMetaBuf;

    dataStream = new FileInputStream(args[1]);

    dataChannel = dataStream.getChannel();

    final int CHECKSUMS_PER_BUF = 1024 * 32;

    metaBuf = ByteBuffer.allocate(checksum.getChecksumSize() * CHECKSUMS_PER_BUF);

    newMetaBuf = ByteBuffer.allocate(checksum.getChecksumSize() * CHECKSUMS_PER_BUF);

    dataBuf = ByteBuffer.allocate(checksum.getBytesPerChecksum() * CHECKSUMS_PER_BUF);

    FileOutputStream fos =
        new FileOutputStream("/Users/wchevreuil/Downloads/new_meta" + System.currentTimeMillis());

    DataOutputStream dos = new DataOutputStream(fos);

    BlockMetadataHeader.writeHeader(dos, newChecksum);

    long offset = 0;

    while (true) {

      dataBuf.clear();

      int dataRead = -1;

      dataRead = dataChannel.read(dataBuf);

      if (dataRead < 0) {
        break;
      }

      int csumToRead =
          (((checksum.getBytesPerChecksum() - 1) + dataRead) / checksum.getBytesPerChecksum())
              * checksum.getChecksumSize();

      metaBuf.clear();

      newMetaBuf.clear();

      metaBuf.limit(csumToRead);

      newMetaBuf.limit(csumToRead);

      metaChannel.read(metaBuf);

      dataBuf.flip();

      metaBuf.flip();

      newMetaBuf.flip();

      System.out.println("offset: " + offset + ", data read: " + dataRead);

      checksum.verifyChunkedSums(dataBuf, metaBuf, args[1], offset);

      newChecksum.calculateChunkedSums(dataBuf, newMetaBuf);

      System.out.println("Checksum value:" + checksum.getValue());

      System.out.println("New Checksum value: " + newChecksum.getValue());

      newChecksum.writeValue(dos, true);

      newChecksum.reset();

      offset += dataRead;
    }

    System.out.println("gonna try generate new meta...");

    dos.flush();

    fos.flush();

    dos.close();

    fos.close();

  }

}
