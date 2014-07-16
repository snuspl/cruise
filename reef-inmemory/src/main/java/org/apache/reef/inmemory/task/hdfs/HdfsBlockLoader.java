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
import org.apache.reef.inmemory.common.exceptions.ConnectionFailedException;
import org.apache.reef.inmemory.common.exceptions.TransferFailedException;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.BlockLoader;

import javax.inject.Inject;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This class loads one block from DataNode
 */
public class HdfsBlockLoader implements BlockLoader {
  private static final Logger LOG = Logger.getLogger(HdfsBlockLoader.class.getName());

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

  /**
   * Constructor of BlockLoader
   */
  @Inject
  public HdfsBlockLoader(final HdfsBlockId id,
                         final HdfsDatanodeInfo dnInfo) {
    hdfsBlockId = id;
    block = new ExtendedBlock(id.getPoolId(), id.getBlockId(), id.getBlockSize(), id.getGenerationTimestamp());
    datanode = new DatanodeID(dnInfo.getIpAddr(), dnInfo.getHostName(), dnInfo.getStorageID(), dnInfo.getXferPort(), dnInfo.getInfoPort(), dnInfo.getInfoSecurePort(), dnInfo.getIpcPort());
    blockSize = id.getBlockSize();
  }

  /**
   * Loading a block from HDFS.
   * Too large block size(>2GB) is not supported.
   */
  public byte[] loadBlock() throws ConnectionFailedException, TokenDecodeFailedException, TransferFailedException {
    final byte[] buf;
    final Configuration conf = new HdfsConfiguration();

    // Allocate a Byte array of the Block size.
    if(blockSize > Integer.MAX_VALUE)
      throw new UnsupportedOperationException("Currently we don't support large(>2GB) block");
    buf = new byte[(int)blockSize];

    LOG.log(Level.INFO, "Start loading block {0} from datanode at {1}",
      new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});

    // Establish a connection against the DataNode
    InetSocketAddress targetAddress;
    Socket socket;
    try {
      targetAddress = NetUtils.createSocketAddr(datanode.getXferAddr());
      socket = NetUtils.getDefaultSocketFactory(conf).createSocket();
      socket.connect(targetAddress, HdfsServerConstants.READ_TIMEOUT);
      socket.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);
    } catch (IOException e) {
      throw new ConnectionFailedException(e);
    }

    // Reproduce BlockToken from BlockId to read the block
    final Token<BlockTokenIdentifier> blockToken;
    try {
      Token<BlockTokenIdentifier> tempToken;
      tempToken = new Token<>();
      tempToken.decodeFromUrlString(hdfsBlockId.getEncodedToken());
      blockToken = tempToken;
    } catch (IOException e) {
      throw new TokenDecodeFailedException(e);
    }

    // Set up BlockReader.
    // TODO Tweak config to improve the performance (e.g. utilize local short-circuit/task)
    BlockReader blockReader;
    String fileName = targetAddress.toString() + ":" + block.getBlockId();
    try {
      blockReader = BlockReaderFactory.newBlockReader(
        new DFSClient.Conf(conf), fileName, block,
        blockToken, START_OFFSET, blockSize,
        VERIFY_CHECKSUM, CLIENT_NAME, TcpPeerServer.peerFromSocket(socket),
        datanode, null, null, null, ALLOW_SHORT_CIRCUIT_LOCAL_READS,
        CachingStrategy.newDefaultStrategy());
    } catch (IOException e) {
      throw new ConnectionFailedException(e);
    }

    LOG.log(Level.INFO, "Data transfer loading block {0} from datanode at {1}",
      new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});

    // Read the data using byte array buffer. BlockReader supports a method
    // to read the data into ByteBuffer directly, but it caused timeout.
    try {
      int totalRead = 0;
      do {
        int nRead = blockReader.read(buf, totalRead, buf.length - totalRead);
        totalRead += nRead;
      } while(totalRead < blockSize);
    } catch (IOException e) {
      throw new TransferFailedException(e);
    }

    // Close the BlockReader when done. It will be closed when the socket has closed.
    try {
      blockReader.close();
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Closing BlockReader for block {0} from datanode at {1} has failed",
        new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});
    }

    LOG.log(Level.INFO, "Done loading block {0} from datanode at {1}",
      new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});
    return buf;
  }
  public BlockId getBlockId() {
    return this.hdfsBlockId;
  }
}