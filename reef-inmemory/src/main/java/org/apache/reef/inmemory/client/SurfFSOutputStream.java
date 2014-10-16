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
public class SurfFSOutputStream extends OutputStream {
  private Path path;
  private SurfMetaService.Client metaClient;
  private byte localBuf[];
  private byte packeBuf[];

  /**
   * This constructor is called outside with the information to create a file
   * @throws IOException If the file exists already
   */
  public SurfFSOutputStream(Path path, SurfMetaService.Client metaClient, long blockSize) throws IOException, TException {
    // Assumption : File is visible right after it is created
    this.path = path;
    this.metaClient = metaClient;

    FileMeta fileMeta = new FileMeta();
    fileMeta.setFullPath(path.toString());
    fileMeta.setBlockSize(blockSize);
    fileMeta.setBlocks(new ArrayList<BlockInfo>());
    fileMeta.setFileSize(0);
    metaClient.registerFileMeta(fileMeta);
  }

  @Override
  public void write(int b) throws IOException {
  }

  @Override
  public void close() throws IOException {
    // TODO Investigate what a proper action is
    super.close();
  }

  class DataStreamer implements Runnable {
    @Override
    public void run() {
      while(true) {
        FileMeta
        BlockInfo
        // Address = metaClient.allocateBlock(path)
        // client = SurfCacheService.getClient(address)
        // client.writeData(new BlockId(file+offset), offset, data)
        this.wait(5);
      }

    }
  }
}