package org.apache.reef.inmemory.driver.hdfs;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.driver.BlockLocationGetter;

import javax.inject.Inject;
import java.io.FileNotFoundException;
import java.io.IOException;

/**
 * Implementation of BlockLocationGetter for HDFS. Each file in HDFS is identified by {@link org.apache.hadoop.fs.Path},
 * and the block locations is returned as {@link org.apache.hadoop.hdfs.protocol.LocatedBlocks}.
 */
public final class HdfsBlockLocationGetter implements BlockLocationGetter<Path, LocatedBlocks> {
  private final FileSystem fs;

  @Inject
  public HdfsBlockLocationGetter(final FileSystem fs) {
    this.fs = fs;
  }

  @Override
  public LocatedBlocks getBlockLocations(final Path path) throws IOException {
    final DFSClient dfsClient = new DFSClient(fs.getUri(), fs.getConf());
    final String pathStr = path.toUri().getPath();

    final HdfsFileStatus hdfsFileStatus = dfsClient.getFileInfo(pathStr);
    if (hdfsFileStatus == null) {
      throw new FileNotFoundException(pathStr);
    }
    final long len = hdfsFileStatus.getLen();

    final LocatedBlocks locatedBlocks = dfsClient.getLocatedBlocks(pathStr, 0, len);
    return locatedBlocks;
  }
}
