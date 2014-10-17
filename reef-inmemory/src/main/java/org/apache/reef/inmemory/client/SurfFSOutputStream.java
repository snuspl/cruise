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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Queue;

/**
 * Output stream implementation. Register metadata when created,
 * and as Client writes the data allocate blocks and transfer the
 * data to the target block. When it fills the block, update metadata
 * and repeat this step until close is called.
 */
public class SurfFSOutputStream extends OutputStream {
  private DataStreamer streamer;
  private Path path;
  private SurfMetaService.Client metaClient;

  private byte localBuf[];
  private int count;

  private Queue<Packet> packetQueue;

  /**
   * This constructor is called outside with the information to create a file
   * @throws IOException If the file exists already
   */
  public SurfFSOutputStream(Path path, SurfMetaService.Client metaClient, long blockSize) throws IOException, TException {
    this.path = path;
    this.metaClient = metaClient;
    this.localBuf = new byte[packetsize]; // 512B(packet size) X 80(queue size) = 40KB
    this.count = 0;
    this.streamer = new DataStreamer();
  }

  @Override
  public void write(int b) throws IOException {
    localBuf[count++] = (byte)b;
    if (count == localBuf.length) {
      flush();
    }
 }

  @Override
  public void flush() throws IOException {
    packetQueue.add(new Packet(localBuf));
    count = 0;
  }

  @Override
  public void close() throws IOException {
    // TODO Investigate what a proper action is
    flush();
  }

  private class DataStreamer implements Runnable {
    @Override
    public void run() {
      while(true) {
        if (packetQueue.size() != 0) {
          try {
            final String address = metaClient.allocateBlock(path.toString());
            final HostAndPort taskAddress = HostAndPort.fromString(address);
            final TTransport transport = new TFramedTransport(new TSocket(taskAddress.getHostText(), taskAddress.getPort()));
            transport.open();
            final TProtocol protocol = new TCompactProtocol(transport);
            SurfCacheService.Client cacheClient = new SurfCacheService.Client(protocol);

            Packet packet = packetQueue.remove();
            cacheClient.writeData(packet.blockId, packet.offset, ByteBuffer.wrap(packet.buf)); // BlockId
          } catch (TException e) {
          }
        }
      }
    }
  }

  private class Packet {
    final long blockId;
    final long offset;
    final byte[] buf;

    public Packet(final long blockId, final long offset, final byte[] buf) {
      this.blockId = blockId;
      this.offset = offset;
      this.buf = buf;
    }
  }
}
