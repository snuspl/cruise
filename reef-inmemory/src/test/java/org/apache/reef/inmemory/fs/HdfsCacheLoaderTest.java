package org.apache.reef.inmemory.fs;

import com.microsoft.reef.driver.catalog.NodeDescriptor;
import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.evaluator.EvaluatorDescriptor;
import com.microsoft.reef.driver.task.RunningTask;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.junit.*;
import org.omg.PortableInterceptor.ACTIVE;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for HdfsCacheLoader. All Hdfs operations are performed on a live
 * MiniDFSCluster.
 */
public final class HdfsCacheLoaderTest {

  private MiniDFSCluster cluster;
  private FileSystem fs;
  private HdfsCacheManager manager;
  private HdfsCacheLoader loader;
  private HdfsTaskSelectionPolicy selector;

  private RunningTask getMockRunningTask(String hostString) {
    RunningTask runningTask = mock(RunningTask.class);
    ActiveContext activeContext = mock(ActiveContext.class);
    EvaluatorDescriptor evaluatorDescriptor = mock(EvaluatorDescriptor.class);
    NodeDescriptor nodeDescriptor = mock(NodeDescriptor.class);
    // Mockito can't mock the final method getHostString(), so using real object
    InetSocketAddress inetSocketAddress = new InetSocketAddress(hostString, 18001);

    doReturn(activeContext).when(runningTask).getActiveContext();
    doReturn(evaluatorDescriptor).when(activeContext).getEvaluatorDescriptor();
    doReturn(nodeDescriptor).when(evaluatorDescriptor).getNodeDescriptor();
    doReturn(inetSocketAddress).when(nodeDescriptor).getInetSocketAddress();

    return runningTask;
  }

  @Before
  public void setUp() throws IOException {
    selector = mock(HdfsTaskSelectionPolicy.class);
    manager = new HdfsCacheManager(selector, 18001);

    List<RunningTask> tasksToCache = new ArrayList<>(3);
    for (int i = 0; i < 3; i++) {
      RunningTask task = getMockRunningTask("host"+i+":18001");
      manager.getCacheAddress(task);
      tasksToCache.add(task);
    }
    when(selector.select(any(LocatedBlock.class), any(Collection.class))).thenReturn(tasksToCache);

    Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);

    cluster = new MiniDFSCluster.Builder(hdfsConfig).numDataNodes(3).build();
    cluster.waitActive();
    fs = cluster.getFileSystem();

    loader = new HdfsCacheLoader(manager, fs.getUri().toString());
  }

  @After
  public void tearDown() {
    cluster.shutdown();
  }

  /**
   * Test load of a non-existing path correctly throws FileNotFoundException
   */
  @Test
  public void testLoadNonexistingPath() {
    try {
      loader.load(new Path("/nonexistent/path"));
      fail("FileNotFoundException was expected");
    } catch (Exception e) {
      assertTrue("Unexpected exception "+e, e instanceof FileNotFoundException);
    }
  }

  /**
   * Test load of a directory (not a file) correctly throws FileNotFoundException
   * @throws IOException
   */
  @Test
  public void testLoadDirectory() throws IOException {
    Path directory = new Path("/existing/directory");

    fs.mkdirs(directory);
    try {
      FileMeta fileMeta = loader.load(directory);
      fail("FileNotFoundException was expected");
    } catch (Exception e) {
      assertTrue("Unexpected exception "+e, e instanceof FileNotFoundException);
    }
  }

  /**
   * Test proper loading of a small file. Checks that metadata is returned,
   * and correct.
   * @throws IOException
   */
  @Test
  public void testLoadSmallFile() throws IOException {
    Path smallFile = new Path("/existing/file");

    FSDataOutputStream outputStream = fs.create(smallFile);
    outputStream.write(1);
    outputStream.close();

    FileMeta fileMeta = loader.load(smallFile);
    assertNotNull(fileMeta);
    assertNotNull(fileMeta.getBlocks());
    assertEquals(3, fileMeta.getBlocksIterator().next().getLocationsSize());

  }
}
