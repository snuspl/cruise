package org.apache.reef.inmemory.client;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.reef.inmemory.task.service.SurfCacheServer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

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
  private byte packetBuf[];

  /**
   * This constructor is called outside with the information to create a file
   * @throws IOException If the file exists already
   */
  public SurfFSOutputStream(Path path, SurfMetaService.Client metaClient, long blockSize) throws IOException, TException {
    this.path = path;
    this.metaClient = metaClient;

    // move the filemeta logic to the Driver
    FileMeta fileMeta = new FileMeta();
    fileMeta.setFullPath(path.toString());
    fileMeta.setBlockSize(blockSize);
    fileMeta.setBlocks(new ArrayList<BlockInfo>());
    fileMeta.setFileSize(0);
    metaClient.registerFileMeta(fileMeta);
  }

  @Override
  public void write(int b) throws IOException {
    // if localBuf full, flush
 }

  @Override
  public void flush() throws IOException {
    // localBuf --> packetBuf
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
        if (packetBuf.length > 0) {
          final String address = metaClient.allocateBlock(path);
          final HostAndPort taskAddress = HostAndPort.fromString(address);
          final TTransport transport = new TFramedTransport(new TSocket(taskAddress.getHostText(), taskAddress.getPort()));
          try {
            transport.open();
          } catch (TTransportException e) {

          }
          final TProtocol protocol = new TCompactProtocol(transport);
          SurfCacheService.Client cacheClient = new SurfCacheService.Client(protocol);
          cacheClient.writeData(block);
        }

        this.wait(5);
      }
    }
  }
}