package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.driver.BlockLocationGetter;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Implementation of BlockLocationGetter for HDFS. Each file in HDFS is identified by {@link org.apache.hadoop.fs.Path},
 * and the block locations is returned as {@link org.apache.hadoop.hdfs.protocol.LocatedBlocks}.
 */
public final class HdfsBlockLocationGetter implements BlockLocationGetter<Path, List<LocatedBlock>> {
  private final FileSystem fs;

  @Inject
  public HdfsBlockLocationGetter(final FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public List<LocatedBlock> getBlockLocations(final Path path) throws IOException {
    final DFSClient dfsClient = new DFSClient(fs.getUri(), fs.getConf());
    final String pathStr = path.toUri().getPath();

    final HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(pathStr);
    if (hdfsFileStatus == null) {
      throw new FileNotFoundException(pathStr);
    }
    final long fileLength = hdfsFileStatus.getLen();
    final long blockSize = hdfsFileStatus.getBlockSize();

    final List<LocatedBlock> blocks = new ArrayList<>();
    long offset = 0;
    while (offset < fileLength) {
      final LocatedBlocks curBlocks = dfsClient.getLocatedBlocks(pathStr, offset);
      blocks.addAll(curBlocks.getLocatedBlocks());
      offset = curBlocks.getLastLocatedBlock().getStartOffset() + blockSize;
    }
    return blocks;
  }
}
