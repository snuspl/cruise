package org.apache.reef.inmemory.task.hdfs;

import com.microsoft.tang.Injector;
import com.microsoft.tang.JavaConfigurationBuilder;
import com.microsoft.tang.Tang;
import com.microsoft.tang.exceptions.InjectionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.*;
import org.apache.hadoop.hdfs.protocol.ClientProtocol;
import org.apache.hadoop.hdfs.protocol.HdfsFileStatus;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.hdfs.protocol.LocatedBlocks;
import org.apache.reef.inmemory.task.BlockLoader;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HdfsBlockLoaderTest {
  private static final String PATH = "/temp/HDFSBlockLoaderTest";
  private static final int NUM_BLOCK = 3;
  private static final int LONG_BYTES = 8;

  private MiniDFSCluster cluster;
  private FileSystem fs;

  @Before
  public void setUp() throws Exception {
    // Initialize the cluster and write sequential numbers over the blocks to check validity of the data loaded
    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Reduce blocksize to 512 bytes, to test multiple blocks
    hdfsConfig.setInt(DFSConfigKeys.DFS_NAMENODE_MIN_BLOCK_SIZE_KEY, 512);
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, 512);

    cluster = new MiniDFSCluster.Builder(hdfsConfig).numDataNodes(3).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    writeSequentialData(PATH, NUM_BLOCK);
  }

  @After
  public void tearDown() throws Exception {
    cluster.shutdown();
  }

  @Test
  public void testLoadBlock() throws IOException, InjectionException {
    long blockSize = fs.getDefaultBlockSize(new Path(PATH));

    DFSClient dfsClient = new DFSClient(cluster.getURI(), new Configuration());
    ClientProtocol nameNode = dfsClient.getNamenode();
    HdfsFileStatus status =  nameNode.getFileInfo(PATH);
    LocatedBlocks blocks = nameNode.getBlockLocations(PATH, 0, status.getLen());
    Assert.assertEquals("The number of blocks should be same as the initial value", NUM_BLOCK, blocks.getLocatedBlocks().size());

    // Because the sequential numbers are written in the file,
    // We are aware the exact position of each number.
    // In this way, we can figure out all the blocks are successfully loaded
    for(int blockIndex = 0; blockIndex < NUM_BLOCK; blockIndex++) {
      LocatedBlock block = blocks.get(blockIndex);
      Assert.assertEquals("Test block size : ", blockSize, block.getBlockSize());

      // Retrieve the information for block and datanode
      HdfsBlockId blockId = HdfsBlockId.copyBlock(block);
      HdfsDatanodeInfo datanodeInfo = HdfsDatanodeInfo.copyDatanodeInfo(block.getLocations()[0]);

      // Instantiate HdfsBlockLoader via TANG
      BlockLoader loader;
      JavaConfigurationBuilder cb = Tang.Factory.getTang().newConfigurationBuilder();
      cb.bindImplementation(BlockLoader.class, HdfsBlockLoader.class);
      Injector i = Tang.Factory.getTang().newInjector(cb.build());
      i.bindVolatileInstance(HdfsBlockId.class, blockId);
      i.bindVolatileInstance(HdfsDatanodeInfo.class, datanodeInfo);
      loader = i.getInstance(BlockLoader.class);

      // Load the data as a ByteBuffer
      ByteBuffer loadedBuf = ByteBuffer.wrap(loader.loadBlock());

      // Because the size of long is 8 bytes, the offset should be calculated as lIndex * 8
      for(long lIndex = 0; lIndex < block.getBlockSize() / LONG_BYTES; lIndex++)
        Assert.assertEquals(String.format("Test the %d th long in %d th block", lIndex, blockIndex), lIndex + blockIndex, loadedBuf.getLong((int)lIndex * LONG_BYTES));
    }
  }

  /**
   * Write sequential numbers of Long type into the file
   * @param path the location of file
   * @param numBlock number of blocks the file takes
   * @throws IOException
   */
  public void writeSequentialData(String path, int numBlock) throws IOException {
    DistributedFileSystem fs = cluster.getFileSystem();
    FSDataOutputStream os = fs.create(new Path(path), true);
    for(int index = 0; index < numBlock; index++) {
      for(long offset = 0; offset < fs.getDefaultBlockSize() / LONG_BYTES; offset++)
        os.writeLong(offset+index);
    }
    os.close();
  }
}