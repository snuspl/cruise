package org.apache.reef.inmemory.task.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.net.Peer;
import org.apache.hadoop.hdfs.net.TcpPeerServer;
import org.apache.hadoop.hdfs.protocol.DatanodeID;
import org.apache.hadoop.hdfs.protocol.DatanodeInfo;
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
import java.util.ArrayList;
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

  private final HdfsBlockId hdfsBlockId;
  private final ExtendedBlock block;
  private final List<HdfsDatanodeInfo> dnInfoList;
  private final long blockSize;
  private final boolean pinned;
  private final int bufferSize;

  private List<byte[]> data = null;
  private int totalRead;

  /**
   * Constructor of BlockLoader
   */
  public HdfsBlockLoader(final HdfsBlockId id,
                         final List<HdfsDatanodeInfo> infoList,
                         final boolean pin,
                         final int bufferSize) {
    hdfsBlockId = id;
    block = new ExtendedBlock(id.getPoolId(), id.getUniqueId(), id.getBlockSize(), id.getGenerationTimestamp());
    dnInfoList = infoList;
    blockSize = id.getBlockSize();
    data = new ArrayList<>();
    totalRead = 0;
    this.pinned = pin;
    this.bufferSize = bufferSize;
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

    Iterator<HdfsDatanodeInfo> dnInfoIter = dnInfoList.iterator();
    do {
      // Prepare the variables to be used - datanodeInfo, datanode, targetAddress, fileName.
      HdfsDatanodeInfo datanodeInfo = dnInfoIter.next();
      DatanodeID datanode = new DatanodeID(datanodeInfo.getIpAddr(), datanodeInfo.getHostName(), datanodeInfo.getDatanodeUuid(),
        datanodeInfo.getXferPort(), datanodeInfo.getInfoPort(), datanodeInfo.getInfoSecurePort(), datanodeInfo.getIpcPort());
      InetSocketAddress targetAddress = NetUtils.createSocketAddr(datanode.getXferAddr());
      String fileName = targetAddress.toString() + ":" + block.getBlockId();

      LOG.log(Level.INFO, "Start loading block {0} from datanode at {1}",
        new String[]{Long.toString(hdfsBlockId.getUniqueId()), datanode.getXferAddr()});

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
        while (totalRead < blockSize) {
          int length = Math.min((int)blockSize - totalRead, bufferSize);
          byte[] buf = new byte[length];
          int nRead = readChunk(blockReader, buf, datanode);
          totalRead += nRead;
          data.add(buf);
        }
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
          new String[]{Long.toString(hdfsBlockId.getUniqueId()), datanode.getXferAddr()});
      } catch (IOException e) {
        LOG.log(Level.WARNING, "Closing BlockReader for block {0} from datanode at {1} has failed",
          new String[]{Long.toString(hdfsBlockId.getUniqueId()), datanode.getXferAddr()});
      }
      break;

    } while (dnInfoIter.hasNext());
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
    final Socket socket;
    try {
      socket = NetUtils.getDefaultSocketFactory(conf).createSocket();
      socket.connect(targetAddress, HdfsServerConstants.READ_TIMEOUT);
      socket.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Connection error while loading block {0} from datanode at {1}. Retry with next datanode",
        new String[]{Long.toString(hdfsBlockId.getUniqueId()), datanode.getXferAddr()});
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
        new String[]{Long.toString(hdfsBlockId.getUniqueId()), datanode.getXferAddr()});
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
  private BlockReader getBlockReader(final Configuration conf, String fileName, Token<BlockTokenIdentifier> blockToken,
                                     Socket socket, DatanodeID datanode) throws ConnectionFailedException {
    /*
     * As BlockReaderFactory is changed to use setX style rather than using Constructor,
     * creating BlockReader has been more complex. RemotePeerFactory is responsible to connect
     * to remote peer. Here is the most simplest implementation for the interface.
     */
    final BlockReader blockReader;
    try {
      blockReader = new BlockReaderFactory(new DFSClient.Conf(conf))
        .setInetSocketAddress(NetUtils.createSocketAddr(datanode.getXferAddr()))
        .setBlock(block)
        .setFileName(fileName)
        .setBlockToken(blockToken)
        .setConfiguration(conf)
        .setStartOffset(START_OFFSET)
        .setLength(blockSize)
        .setVerifyChecksum(VERIFY_CHECKSUM)
        .setClientName(CLIENT_NAME)
        .setDatanodeInfo(new DatanodeInfo(datanode))
        .setClientCacheContext(ClientContext.getFromConf(conf))
        .setCachingStrategy(CachingStrategy.newDefaultStrategy())
        .setRemotePeerFactory(new RemotePeerFactory() {
          @Override
          public Peer newConnectedPeer(InetSocketAddress addr) throws IOException {
            Peer peer = null;
            Socket sock = NetUtils.getDefaultSocketFactory(conf).createSocket();
            try {
              sock.connect(addr, HdfsServerConstants.READ_TIMEOUT);
              sock.setSoTimeout(HdfsServerConstants.READ_TIMEOUT);
              peer = TcpPeerServer.peerFromSocket(sock);
            } finally {
              if (peer == null) {
                org.apache.hadoop.io.IOUtils.closeSocket(sock);
              }
            }
            return peer;
          }
        })
        .build();
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Connection error while loading block {0} from datanode at {1}. Retry with next datanode",
        new String[]{Long.toString(hdfsBlockId.getUniqueId()), datanode.getXferAddr()});
      throw new ConnectionFailedException(e);
    }
    return blockReader;
  }

  /**
   * Read a chunk of block from Datanode
   * @param blockReader BlockReader to load the data (from hadoop package)
   * @param buf Byte buffer to load bytes
   * @param datanode Datanode to load the data from
   * @throws TransferFailedException When an error occurred while transferring data
   */
  private int readChunk(BlockReader blockReader, byte[] buf, DatanodeID datanode) throws TransferFailedException {
    try {
      return blockReader.readAll(buf, 0, buf.length);
    } catch (IOException e) {
      LOG.log(Level.WARNING, "Data transfer error while loading block {0} from datanode at {1}. Retry with next datanode",
        new String[]{Long.toString(hdfsBlockId.getUniqueId()), datanode.getXferAddr()});
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

  /**
   * @return {@code true} if the block is requested to pin.
   */
  @Override
  public boolean isPinned() {
    return pinned;
  }

  /**
   * @return Size of buffer used in block loading
   */
  @Override
  public int getBufferSize() {
    return bufferSize;
  }

  /**
   * Get data split from cache with an offset of {@code index * bufferSize}
   * @param index Index of the chunk to load
   * @return Cached byte array
   * @throws BlockLoadingException
   */
  @Override
  public byte[] getData(int index) throws BlockLoadingException {
    if (blockSize <= bufferSize * index) {
      throw new IndexOutOfBoundsException("The requested index exceeded the capacity.");
    } else if (data == null || index >= data.size()) {
      throw new BlockLoadingException(totalRead);
    }
    return data.get(index);
  }
}