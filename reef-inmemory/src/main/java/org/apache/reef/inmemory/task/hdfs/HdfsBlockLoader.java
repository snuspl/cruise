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
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.ConnectionFailedException;
import org.apache.reef.inmemory.common.exceptions.TransferFailedException;
import org.apache.reef.inmemory.task.BlockId;
import org.apache.reef.inmemory.task.BlockLoader;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Iterator;
import java.util.List;
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

  private final HdfsBlockId hdfsBlockId;
  private final ExtendedBlock block;
  private final List<HdfsDatanodeInfo> dnInfoList;
  private final long blockSize;

  private byte[] data = null;
  private int totalRead;

  /**
   * Constructor of BlockLoader
   */
  public HdfsBlockLoader(final HdfsBlockId id,
                         final List<HdfsDatanodeInfo> infoList) {
    hdfsBlockId = id;
    block = new ExtendedBlock(id.getPoolId(), id.getBlockId(), id.getBlockSize(), id.getGenerationTimestamp());
    dnInfoList = infoList;
    blockSize = id.getBlockSize();
    totalRead = 0;
  }

  /**
   * Loading a block from HDFS.
   * Too large block size(>2GB) is not supported.
   */
  @Override
  public void loadBlock() throws IOException {

    final Configuration conf = new HdfsConfiguration();

    // Allocate a Byte array of the Block size.
    if(blockSize > Integer.MAX_VALUE)
      throw new UnsupportedOperationException("Currently we don't support large(>2GB) block");
    final byte[] buf;
    try {
      buf = new byte[(int) blockSize];
    } catch (OutOfMemoryError e) {
      LOG.log(Level.SEVERE, "Closing BlockReader for block {0} from datanode at {1} has failed", e);
      throw new IOException(e);
    }

    Iterator<HdfsDatanodeInfo> dnInfoIter = dnInfoList.iterator();
    do {
      // Prepare the variables to be used - datanodeInfo, datanode, targetAddress, fileName.
      HdfsDatanodeInfo datanodeInfo = dnInfoIter.next();
      DatanodeID datanode = new DatanodeID(datanodeInfo.getIpAddr(), datanodeInfo.getHostName(), datanodeInfo.getStorageID(),
        datanodeInfo.getXferPort(), datanodeInfo.getInfoPort(), datanodeInfo.getInfoSecurePort(), datanodeInfo.getIpcPort());
      InetSocketAddress targetAddress = NetUtils.createSocketAddr(datanode.getXferAddr());
      String fileName = targetAddress.toString() + ":" + block.getBlockId();

      LOG.log(Level.INFO, "Start loading block {0} from datanode at {1}",
        new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});

      // Declare socket and blockReader object to close them in the future.
      Socket socket = null;
      final BlockReader blockReader;

      try {
        // Connect to Datanode and create a Block reader
        final Token<BlockTokenIdentifier> blockToken = decodeBlockToken(hdfsBlockId, datanode);
        socket = connectToDatanode(conf, targetAddress, datanode);
        blockReader = getBlockReader(conf, fileName, blockToken, socket, datanode);

        LOG.log(Level.INFO, "Transfer the data from datanode at {0}", datanode.getXferAddr());

        // Transfer data to read the block
        readBlock(blockReader, buf, datanode);
      } catch (TokenDecodeFailedException | ConnectionFailedException | TransferFailedException ex) {
        if (dnInfoIter.hasNext()) {
          closeSocketToRetry(socket);
          continue;
        }
        throw ex;
      }

      // If loading done, close the connections and break the while loop.
      try {
        blockReader.close();
        socket.close();
        LOG.log(Level.INFO, "Done loading block {0} from datanode at {1}",
          new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Closing BlockReader for block {0} from datanode at {1} has failed",
          new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});
      }
      break;

    } while(dnInfoIter.hasNext());
    data = buf;
  }

  /**
   * Return the block id which is assigned to this Loader
   * @return The Id of block
   */
  public BlockId getBlockId() {
    return this.hdfsBlockId;
  }

  /**
   * Connect to Datanode
   * @param conf Hadoop Configuration
   * @param targetAddress Socket address to connect to Datanode
   * @param datanode Datanode to connect to
   * @return Socket object connecting to datanode
   * @throws ConnectionFailedException When failed to to connect to Datanode
   */
  private Socket connectToDatanode(Configuration conf, InetSocketAddress targetAddress, DatanodeID datanode)
    throws ConnectionFailedException {
    Socket socket;
    try {
      socket = NetUtils.getDefaultSocketFactory(conf).createSocket();
      socket.connect(targetAddress, HdfsServerConstants.READ_TIMEOUT);
      socket.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Connection error while loading block {0} from datanode at {1}. Retry with next datanode",
        new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});
      throw new ConnectionFailedException(e);
    }

    return socket;
  }

  /**
   * Decode a BlockToken which is necessary to create BlockReader. Because Token itself is not serializable,
   * the Token is encoded when stored in {@link org.apache.reef.inmemory.task.hdfs.HdfsBlockId}
   * @param hdfsBlockId BlockId of the block to read
   * @param datanode Datanode to load the data from
   * @return Token Identifier used to load the block
   * @throws TokenDecodeFailedException When failed to decode the token
   */
  private Token<BlockTokenIdentifier> decodeBlockToken(HdfsBlockId hdfsBlockId, DatanodeID datanode) throws TokenDecodeFailedException {
    Token<BlockTokenIdentifier> blockToken;
    blockToken = new Token<>();
    try {
      blockToken.decodeFromUrlString(hdfsBlockId.getEncodedToken());
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Token decode error while loading block {0} from datanode at {1}. Retry with next datanode",
        new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});
      throw new TokenDecodeFailedException(e);
    }
    return blockToken;
  }

  /**
   * Retrieve a BlockReader (included in hadoop.hdfs package)
   * @param conf Hadoop Configuration
   * @param fileName Filename which the block belongs to
   * @param blockToken Token Identifier used to load the block
   * @param socket Socket object connecting to datanode
   * @param datanode Datanode to load the data from
   * @return BlockReader object to read data
   * @throws ConnectionFailedException When failed to to connect to Datanode
   */
  private BlockReader getBlockReader(Configuration conf, String fileName, Token<BlockTokenIdentifier>blockToken,
                                     Socket socket, DatanodeID datanode) throws ConnectionFailedException {
    BlockReader blockReader;
    try {
      // TODO Tweak config to improve the performance (e.g. utilize local short-circuit/task)
      blockReader = BlockReaderFactory.newBlockReader(
        new DFSClient.Conf(conf), fileName, block,
        blockToken, START_OFFSET, blockSize,
        VERIFY_CHECKSUM, CLIENT_NAME, TcpPeerServer.peerFromSocket(socket),
        datanode, null, null, null, ALLOW_SHORT_CIRCUIT_LOCAL_READS,
        CachingStrategy.newDefaultStrategy());

    } catch (IOException e) {
      LOG.log(Level.WARNING, "Connection error while loading block {0} from datanode at {1}. Retry with next datanode",
        new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});
      throw new ConnectionFailedException(e);
    }
    return blockReader;
  }

  /**
   * Read a block from Datanode
   * @param blockReader BlockReader to load the data (from hadoop package)
   * @param buf Byte array buffer
   * @param datanode Datanode to load the data from
   * @throws TransferFailedException When an error occurred while transferring data
   */
  private void readBlock(BlockReader blockReader, byte[] buf, DatanodeID datanode) throws TransferFailedException {
    try {
      do {
        int nRead = 0;
        nRead = blockReader.read(buf, totalRead, buf.length - totalRead);
        totalRead += nRead;
      } while(totalRead < blockSize);
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Data transfer error while loading block {0} from datanode at {1}. Retry with next datanode",
        new String[]{Long.toString(hdfsBlockId.getBlockId()), datanode.getXferAddr()});
      throw new TransferFailedException(e);
    }
  }

  /**
   * Helper function to close Socket. The catch block is little bit messy with this logic.
   * @param socket Socket object to close
   */
  private void closeSocketToRetry(Socket socket) {
    if (socket!=null) {
      try {
        socket.close();
      } catch (IOException e1) {
        LOG.log(Level.WARNING, "Closing Socket failed. Retry anyway");
      }
    }
  }

  @Override
  public byte[] getData() throws BlockLoadingException {
    if (data == null) {
      throw new BlockLoadingException(totalRead);
    } else {
      return data;
    }
  }
}