package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;

import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;

/**
 * Output stream implementation. Register metadata when created,
 * and as Client writes the data allocate blocks and transfer the
 * data to the target block. When it fills the block, update metadata
 * and repeat this step until close is called.
 */
public class SurfFSOutputStream extends FSDataOutputStream {
  private Path path;
  private SurfMetaService.Client metaClient;

  /**
   * Implementation of OutputStream to work inside the SurfFSOutputStream
   */
  private static class SurfOutputStream extends OutputStream implements Closeable {

    @Override
    public void write(int b) throws IOException {
    }

    @Override
    public void close() throws IOException {
      // TODO Investigate what a proper action is
      super.close();
    }
  }

  /**
   * This constructor is called outside with the information to create a file
   * @throws IOException If the file exists already
   */
  public SurfFSOutputStream(Path path, SurfMetaService.Client metaClient, long blockSize) throws IOException, TException {
    // Assumption : File is visible right after it is created
    this(new SurfOutputStream(), new FileSystem.Statistics("surf"));
    this.path = path;
    this.metaClient = metaClient;

    FileMeta fileMeta = new FileMeta();
    fileMeta.setFullPath(path.toString());
    fileMeta.setBlockSize(blockSize);
    fileMeta.setBlocks(new ArrayList<BlockInfo>());
    fileMeta.setFileSize(0);
    metaClient.registerFileMeta(fileMeta);
  }

  /**
   * This is internal to call the constructor of super class
   */
  private SurfFSOutputStream(OutputStream out, FileSystem.Statistics stats) throws IOException {
    super(out, stats);
  }
}
