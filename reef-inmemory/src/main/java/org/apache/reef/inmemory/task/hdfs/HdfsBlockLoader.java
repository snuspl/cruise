package org.apache.reef.inmemory.task.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.security.token.block.BlockTokenIdentifier;
import org.apache.hadoop.hdfs.server.common.HdfsServerConstants;
import org.apache.hadoop.hdfs.server.datanode.CachingStrategy;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.security.token.Token;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.BlockLoader;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;

/**
 * This class loads one block from DataNode
 */
public class HdfsBlockLoader implements BlockLoader {
  // Some Fields are left null, because those types are not public.
  private static final int START_OFFSET = 0;
  private static final String CLIENT_NAME = "BlockLoader";
  private static final boolean VERIFY_CHECKSUM = true;
  private static final boolean ALLOW_SHORT_CIRCUIT_LOCAL_READS = false;
  private static final CachingStrategy STRATEGY = CachingStrategy.newDefaultStrategy();

  private final HdfsBlockId hdfsBlockId;
  private final ExtendedBlock block;
  private final DatanodeID datanode;
  private final long blockSize;
  private final Token<BlockTokenIdentifier> blockToken;

  /**
   * Constructor of BlockLoader
   */
  @Inject
  public HdfsBlockLoader(final HdfsBlockId id,
                         final HdfsDatanodeInfo dnInfo) throws IOException {
    hdfsBlockId = id;
    block = new ExtendedBlock(id.getPoolId(), id.getBlockId(), id.getBlockSize(), id.getGenerationTimestamp());
    datanode = new DatanodeID(dnInfo.getIpAddr(), dnInfo.getHostName(), dnInfo.getStorageID(), dnInfo.getXferPort(), dnInfo.getInfoPort(), dnInfo.getInfoSecurePort(), dnInfo.getIpcPort());
    blockSize = id.getBlockSize();
    Token<BlockTokenIdentifier> tempToken = null;
    tempToken = new Token<>();
    tempToken.decodeFromUrlString(id.getEncodedToken());
    blockToken = tempToken;
  }

  /**
   * Loading a block from HDFS.
   * Too large block size(>2GB) is not supported.
   */
  public byte[] loadBlock() throws IOException {
    final byte[] buf = new byte[(int)blockSize];
    final Configuration conf = new HdfsConfiguration();

    // Allocate a ByteBuffer as a size of the Block
    if(blockSize > Integer.MAX_VALUE)
      throw new UnsupportedOperationException("Currently we don't support large(>2GB) block");

    // Establish a connection against the DataNode
    InetSocketAddress targetAddress = NetUtils.createSocketAddr(datanode.getXferAddr());
    Socket s = NetUtils.getDefaultSocketFactory(conf).createSocket();
    s.connect(targetAddress, HdfsServerConstants.READ_TIMEOUT);
    s.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);

    String fileName = targetAddress.toString() + ":" + block.getBlockId();

    // Set up BlockReader.
    // TODO Tweak config to improve the performance (e.g. utilize local short-circuit/task)
    BlockReader blockReader = BlockReaderFactory.newBlockReader(
      new DFSClient.Conf(conf), fileName, block,
      blockToken, START_OFFSET, blockSize,
      VERIFY_CHECKSUM, CLIENT_NAME, TcpPeerServer.peerFromSocket(s),
      datanode, null, null, null, ALLOW_SHORT_CIRCUIT_LOCAL_READS,
      CachingStrategy.newDefaultStrategy());

    // Read the data using byte array buffer. BlockReader supports a method
    // to read the data into ByteBuffer directly, but it caused timeout.
    int totalRead = 0;
    do {
      int nRead = blockReader.read(buf, totalRead, buf.length - totalRead);
      totalRead += nRead;
    } while(totalRead < blockSize);
    blockReader.close();

    return buf;
  }
  public BlockId getBlockId() {
    return this.hdfsBlockId;
  }
}