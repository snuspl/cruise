package org.apache.reef.inmemory.task.hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.common.hdfs.HdfsBlockInfoFactory;
import org.apache.reef.inmemory.common.ITUtils;
import org.apache.reef.inmemory.common.exceptions.ConnectionFailedException;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.common.instrumentation.NullEventRecorder;
import org.apache.reef.inmemory.task.BlockLoader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class HdfsBlockLoaderITCase {
  private static final String TESTDIR = ITUtils.getTestDir();
  private static final String PATH = TESTDIR+"/HDFSBlockLoaderTest";
  private static final int NUM_BLOCK = 3;
  private static final int LONG_BYTES = 8;
  private static final int BUFFER_SIZE = 8 * 1024 * 1024;

  private EventRecorder RECORD;
  private HdfsBlockInfoFactory blockInfoFactory;

  private FileSystem fs;
  private LocatedBlocks blocks;

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   */
  @Before
  public void setUp() throws Exception {
    RECORD = new NullEventRecorder();
    blockInfoFactory = new HdfsBlockInfoFactory();

    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Reduce blocksize to 512 bytes, to test multiple blocks
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);

    fs = ITUtils.getHdfs(hdfsConfig);

    // Write sequential numbers over the blocks to check validity of the data loaded
    writeSequentialData(PATH, NUM_BLOCK);

    final DFSClient dfsClient = new DFSClient(fs.getUri(), new Configuration());
    final ClientProtocol nameNode = dfsClient.getNamenode();
    final HdfsFileStatus status =  nameNode.getFileInfo(PATH);
    blocks = nameNode.getBlockLocations(PATH, 0, status.getLen());
    Assert.assertEquals("The number of blocks should be same as the initial value", NUM_BLOCK, blocks.getLocatedBlocks().size());
  }

  /**
   * Remove all directories.
   */
  @After
  public void tearDown() throws Exception {
    fs.delete(new Path(TESTDIR), true);
  }

  /*
   * This case covers the case to load a block successfully
   */
  @Test
  public void testLoadBlock() throws Exception {
    long blockSize = fs.getDefaultBlockSize(new Path(PATH));

    /*
     * Because the sequential numbers are written in the file,
     * We are aware the exact position of each number.
     * In this way, we can figure out all the blocks are successfully loaded
     */
    for(int blockIndex = 0; blockIndex < NUM_BLOCK; blockIndex++) {
      LocatedBlock block = blocks.get(blockIndex);
      Assert.assertEquals("Test block size : ", blockSize, block.getBlockSize());

      // Retrieve the information for block and datanode
      HdfsBlockInfo blockId = blockInfoFactory.newBlockInfo(PATH, block);
      List<HdfsDatanodeInfo> datanodeInfo = HdfsDatanodeInfo.copyDatanodeInfos(block.getLocations());

      BlockLoader loader = new HdfsBlockLoader(blockId, datanodeInfo, false, BUFFER_SIZE, RECORD);

      // Load the data as a ByteBuffer
      loader.loadBlock();
      for(int chunkIndex = 0; chunkIndex * BUFFER_SIZE < blockSize; chunkIndex++) {
        ByteBuffer loadedBuf = ByteBuffer.wrap(loader.getData(chunkIndex));
        // Because the size of long is 8 bytes, the offset should be calculated as lIndex * 8
        for (long lIndex = 0; lIndex < loadedBuf.limit() / LONG_BYTES; lIndex++) {
          Assert.assertEquals(String.format("Test the %d th long in %d th block", lIndex, blockIndex),
            blockIndex + chunkIndex * (blockSize / BUFFER_SIZE) + lIndex, loadedBuf.getLong((int) lIndex * LONG_BYTES));
        }
      }
    }
  }

  /*
   * This test covers the case when the Block size is over the limit
   * Unsupported block size : Integer.MAX_VALUE+1 (>2GB)
   */
  @Test(expected = UnsupportedOperationException.class)
  public void testInvalidSize() throws IOException {
    LocatedBlock block = blocks.get(0);
    HdfsBlockInfo blockId = blockInfoFactory.newBlockInfo(PATH, block);
    HdfsBlockInfo dummyBlockId = new HdfsBlockInfo(PATH, 0, blockId.getUniqueId(), Integer.MAX_VALUE+(long)1, blockId.getGenerationTimestamp(),  blockId.getPoolId(), blockId.getEncodedToken());
    List<HdfsDatanodeInfo> datanodeInfo = HdfsDatanodeInfo.copyDatanodeInfos(block.getLocations());

    BlockLoader loader = new HdfsBlockLoader(dummyBlockId, datanodeInfo, false, BUFFER_SIZE, RECORD);
    loader.loadBlock();
  }

  /*
   * This test covers the case when the DataNode Address is invalid
   * Unreachable address : Wrong ip address and hostname
   */
  @Test(expected = ConnectionFailedException.class)
  public void testInvalidAddress() throws IOException{
    LocatedBlock block = blocks.get(0);

    HdfsBlockInfo blockId = blockInfoFactory.newBlockInfo(PATH, block);
    List<HdfsDatanodeInfo> dummyDatanodeInfo = new ArrayList<HdfsDatanodeInfo>();
    dummyDatanodeInfo.add(new HdfsDatanodeInfo("1.1.1.1", "unreachable", "peer_unreachable", 0, 0, 0, 0));
    BlockLoader loader = new HdfsBlockLoader(blockId, dummyDatanodeInfo, false, BUFFER_SIZE, RECORD);
    loader.loadBlock();
  }

  /*
   * This test covers the case when it needs retrying with another datanode
   * Datanode list : the first datanode info is invalid, others are valid
   */
  @Test
  public void testRetry() throws IOException {
    LocatedBlock block = blocks.get(0);

    HdfsBlockInfo blockId = blockInfoFactory.newBlockInfo(PATH, block);
    List<HdfsDatanodeInfo> datanodeInfo = HdfsDatanodeInfo.copyDatanodeInfos(block.getLocations());
    datanodeInfo.add(0, new HdfsDatanodeInfo("1.1.1.1", "unreachable", "peer_unreachable", 0, 0, 0, 0));
    BlockLoader loader = new HdfsBlockLoader(blockId, datanodeInfo, false, BUFFER_SIZE, RECORD);
    loader.loadBlock();
  }

  /*
   * This case covers when the BlockId is invalid
   * Wrong block id : -1
   */
  @Test(expected = ConnectionFailedException.class)
  public void testInvalidId() throws IOException {
    LocatedBlock block = blocks.get(0);

    HdfsBlockInfo blockId = blockInfoFactory.newBlockInfo(PATH, block);
    HdfsBlockInfo dummyBlockId = new HdfsBlockInfo(PATH, 0, (long)-1, blockId.getBlockSize(), blockId.getGenerationTimestamp(),  blockId.getPoolId(), blockId.getEncodedToken());
    List<HdfsDatanodeInfo> datanodeInfo = HdfsDatanodeInfo.copyDatanodeInfos(block.getLocations());
    BlockLoader loader = new HdfsBlockLoader(dummyBlockId, datanodeInfo, false, BUFFER_SIZE, RECORD);
    loader.loadBlock();
  }

  /*
   * This case covers when the given Token is invalid
   * Wrong Token : pretend the token encoded as a blank string
   */
  @Test(expected = TokenDecodeFailedException.class)
  public void testInvalidToken() throws IOException {
    LocatedBlock block = blocks.get(0);

    HdfsBlockInfo blockId = blockInfoFactory.newBlockInfo(PATH, block);
    HdfsBlockInfo dummyBlockId = new HdfsBlockInfo(PATH, 0, blockId.getUniqueId(), blockId.getBlockSize(), blockId.getGenerationTimestamp(),  blockId.getPoolId(), "");
    List<HdfsDatanodeInfo> datanodeInfo = HdfsDatanodeInfo.copyDatanodeInfos(block.getLocations());
    BlockLoader loader = new HdfsBlockLoader(dummyBlockId, datanodeInfo, false, BUFFER_SIZE, RECORD);
    loader.loadBlock();
  }

  /**
   * Write sequential numbers of Long type into the file
   * @param path the location of file
   * @param numBlock number of blocks the file takes
   * @throws IOException
   */
  public void writeSequentialData(String path, int numBlock) throws IOException {
    FSDataOutputStream os = fs.create(new Path(path), true);
    for(int index = 0; index < numBlock; index++) {
      for(long offset = 0; offset < fs.getDefaultBlockSize() / LONG_BYTES; offset++)
        os.writeLong(offset+index);
    }
    os.close();
  }
}