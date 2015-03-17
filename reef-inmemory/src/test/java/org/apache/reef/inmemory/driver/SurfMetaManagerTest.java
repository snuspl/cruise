package org.apache.reef.inmemory.driver;

import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.hadoop.fs.*;
import org.apache.reef.inmemory.common.*;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.common.hdfs.HdfsFileMetaFactory;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.logging.Logger;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.*;

/**
 * Test class for SurfMetaManager
 */
public final class SurfMetaManagerTest {
  private static final Logger LOG = Logger.getLogger(SurfMetaManagerTest.class.getName());
  private static final long blockSize = 128L * 1024 * 1024;
  final short replication = (short)1;
  private CacheMessenger cacheMessenger;
  private CacheLocationRemover cacheLocationRemover;
  private CacheUpdater cacheUpdater;
  private BlockMetaFactory blockMetaFactory;
  private FileMetaFactory metaFactory;
  private LocationSorter locationSorter;
  private BaseFsClient baseFsClient;

  /**
   * Setup the Meta Manager with a mock CacheLoader that returns
   * blank metadata for each path.
   * @throws Exception
   */
  @Before
  public void setUp() throws Exception {
    cacheMessenger = mockCacheMessenger();
    cacheLocationRemover = new CacheLocationRemover();
    cacheUpdater = mock(CacheUpdater.class);
    blockMetaFactory = mock(BlockMetaFactory.class);
    metaFactory = new HdfsFileMetaFactory();
    locationSorter = mock(LocationSorter.class);
    baseFsClient = mock(BaseFsClient.class);
  }

  private static User defaultUser() {
    final User user = new User();
    user.setOwner("surf");
    user.setGroup("surf");
    return user;
  }

  private static CacheMessenger mockCacheMessenger() {
    final CacheMessenger cacheMessenger = mock(CacheMessenger.class);
    doNothing().when(cacheMessenger).clearAll();
    return cacheMessenger;
  }

  /**
   * Verify that metaLoader#load is called only when the path given does not exist.
   * @throws Throwable
   */
  @Test
  public void testGet() throws Throwable {
    final Path path = new Path("/path");
    final User user = defaultUser();

    final CacheLoader<Path, FileMeta> metaLoader = mock(CacheLoader.class);
    final FileMeta fileMeta = new FileMeta();
    when(metaLoader.load(path)).thenReturn(fileMeta);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(metaLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater,
            blockMetaFactory, metaFactory, locationSorter, baseFsClient);
    when(cacheUpdater.updateMeta(eq(fileMeta))).thenReturn(fileMeta.deepCopy());

    metaManager.get(path, user);
    verify(metaLoader, times(1)).load(path);
    metaManager.get(path, user);
    verify(metaLoader, times(1)).load(path);
  }

  /**
   * Verify that a newly-create file's fileMeta
   * TODO: Test User-related attributes & a file at the root (e.g. surf://hi.txt)
   * @throws Throwable
   */
  @Test
  public void testCreateFile() throws Throwable {
    final User user = defaultUser();
    final Path path = new Path("/path");

    final CacheLoader<Path, FileMeta> metaLoader = mock(CacheLoader.class);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(metaLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();

    final BaseFsClient<FileStatus> baseFsClient = mock(BaseFsClient.class);

    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater,
            blockMetaFactory, metaFactory, locationSorter, baseFsClient);

    // Assume that create from baseFS succeeds
    doNothing().when(baseFsClient).create(anyString(), anyShort(), anyLong());

    // Assume that getting parentMeta succeeds
    final FileMeta parentFileMeta = new FileMeta();
    parentFileMeta.setFullPath(getPathStr(path.getParent()));
    parentFileMeta.setUser(user);
    when(metaLoader.load(path.getParent())).thenReturn(parentFileMeta);

    // Create a file
    assertTrue("No error should be invoked", metaManager.createFile(getPathStr(path), replication, blockSize));
    verify(metaLoader, times(1)).load(path.getParent());

    // Should not load meta as the fileMeta is already created(cached)
    final FileMeta result = metaManager.get(path, user);
    verify(metaLoader, times(0)).load(path);

    // Check validity of fileMeta
    assertNotNull(result);
    assertFalse("Should not be a directory", result.isDirectory());
    assertEquals(getPathStr(path), result.getFullPath());
    assertEquals(replication, result.getReplication());
    assertEquals(blockSize, result.getBlockSize());
    assertEquals(parentFileMeta, metaManager.getParent(result));
  }

  /**
   * Verify concurrent creation of files under the same directory
   */
  @Test
  public void testConcurrentCreateFile() throws Throwable {
    final User user = defaultUser();
    final Path directoryPath = new Path("/path");

    final CacheLoader<Path, FileMeta> metaLoader = mock(CacheLoader.class);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(metaLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();

    final BaseFsClient<FileStatus> baseFsClient = mock(BaseFsClient.class);

    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater,
            blockMetaFactory, metaFactory, locationSorter, baseFsClient);

    // Assume that create from baseFS succeeds
    doNothing().when(baseFsClient).create(anyString(), anyShort(), anyLong());

    // Assume that getting directoryMeta succeeds
    final FileMeta directoryFileMeta = new FileMeta();
    directoryFileMeta.setFullPath(getPathStr(directoryPath));
    directoryFileMeta.setUser(user);
    when(metaLoader.load(directoryPath)).thenReturn(directoryFileMeta);

    final int numFiles = 100;
    final ExecutorService executorService = Executors.newCachedThreadPool();
    final Future<?>[] futures = new Future<?>[numFiles];

    // Concurrently create files under the same directory
    for (int i = 0; i < numFiles; i++) {
      final int index = i;
      futures[index] = executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            final Path filePath = new Path(directoryPath, String.valueOf(index));
            metaManager.createFile(getPathStr(filePath), replication, blockSize);
          } catch (final Throwable t) {
            throw new RuntimeException(t);
          }
        }
      });
    }
    executorService.shutdown();

    // Verify that all operations succeeded
    int successCount = 0;
    for (final Future future : futures) {
      future.get();
      successCount += future.isCancelled() ? 0 : 1;
    }
    assertEquals(numFiles, successCount);

    for (int index = 0; index < numFiles; index++) {
      final Path filePath = new Path(directoryPath, String.valueOf(index));

      // Should not load meta as the fileMeta is already created(cached)
      final FileMeta result = metaManager.get(filePath, user);
      verify(metaLoader, times(0)).load(filePath);

      // Check validity of fileMeta
      assertNotNull(result);
      assertFalse("Should not be a directory", result.isDirectory());
      assertEquals(getPathStr(filePath), result.getFullPath());
      assertEquals(replication, result.getReplication());
      assertEquals(blockSize, result.getBlockSize());
      // children-to-parent mapping
      assertEquals(directoryFileMeta, metaManager.getParent(result));
      // parent-to-children mapping
      assertNotNull(metaManager.getChildren(directoryFileMeta));
      assertTrue("Should be a child of directoryPath, index: " + String.valueOf(index),
              metaManager.getChildren(directoryFileMeta).contains(result));
    }
  }

  /**
   * Verify a newly-created directory's fileMeta
   */
  @Test
  public void testCreateDirectory() throws Throwable {
    final User user = defaultUser();
    final Path rootPath = new Path("/");
    final Path newDirPath = new Path(rootPath, "dir");

    final CacheLoader<Path, FileMeta> metaLoader = mock(CacheLoader.class);
        final FileMeta newDirFileMeta = new FileMeta();
    newDirFileMeta.setFullPath(getPathStr(newDirPath));
    newDirFileMeta.setUser(user);
    newDirFileMeta.setDirectory(true);
    when(metaLoader.load(newDirPath)).thenReturn(newDirFileMeta);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(metaLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();

    final BaseFsClient<FileStatus> baseFsClient = mock(BaseFsClient.class);

    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater,
            blockMetaFactory, metaFactory, locationSorter, baseFsClient);

    when(baseFsClient.exists(getPathStr(rootPath))).thenReturn(true);
    when(baseFsClient.exists(getPathStr(newDirPath))).thenReturn(false);
    // Assume that mkdirs from baseFS succeeds
    when(baseFsClient.mkdirs(anyString())).thenReturn(true);

    // Assume that getting rootFileMeta succeeds
    final FileMeta rootFileMeta = new FileMeta();
    rootFileMeta.setFullPath(getPathStr(rootPath));
    rootFileMeta.setUser(user);
    rootFileMeta.setDirectory(true);
    when(metaLoader.load(rootPath)).thenReturn(rootFileMeta);

    // Create a directory
    assertTrue("No error should be invoked", metaManager.createDirectory(getPathStr(newDirPath)));
    verify(metaLoader, times(1)).load(rootPath);

    // Metadata is loaded once while the directory is being created.
    final FileMeta result = metaManager.get(newDirPath, user);
    verify(metaLoader, times(1)).load(newDirPath);

    // Check validity of fileMeta
    assertNotNull(result);
    assertTrue("Should be a directory", result.isDirectory());
    assertEquals(getPathStr(newDirPath), result.getFullPath());
    assertEquals(rootFileMeta, metaManager.getParent(result));
  }

  /**
   * Verify concurrent creation of directories under the same directory
   */
  @Test
  public void testConcurrentCreateDirectory() throws Throwable{
    final User user = defaultUser();
    final Path rootPath = new Path("/");

    final CacheLoader<Path, FileMeta> metaLoader = mock(CacheLoader.class);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(metaLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();

    final BaseFsClient<FileStatus> baseFsClient = mock(BaseFsClient.class);

    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater,
            blockMetaFactory, metaFactory, locationSorter, baseFsClient);

    // Assume that mkdirs from baseFS succeeds
    when(baseFsClient.mkdirs(anyString())).thenReturn(true);
    when(baseFsClient.exists(anyString())).thenReturn(false);
    when(baseFsClient.exists(getPathStr(rootPath))).thenReturn(true);

    // Assume that getting directoryMeta succeeds
    final FileMeta rootFileMeta = new FileMeta();
    rootFileMeta.setFullPath(getPathStr(rootPath));
    rootFileMeta.setUser(user);
    rootFileMeta.setDirectory(true);
    when(metaLoader.load(rootPath)).thenReturn(rootFileMeta);

    final int numDirs = 100;
    for (int index = 0; index < numDirs; index++) {
      final Path dirPath = new Path(rootPath, String.valueOf(index));
      final FileMeta dirFileMeta = new FileMeta();
      dirFileMeta.setFullPath(getPathStr(dirPath));
      dirFileMeta.setUser(user);
      dirFileMeta.setDirectory(true);
      when(metaLoader.load(dirPath)).thenReturn(dirFileMeta);
    }

    final ExecutorService executorService = Executors.newCachedThreadPool();
    final Future<Boolean>[] futures = new Future[numDirs];

    // Concurrently create directories under the root directory
    for (int i = 0; i < numDirs; i++) {
      final int index = i;
      futures[index] = executorService.submit(new Callable<Boolean>() {
        @Override
        public Boolean call() {
          try {
            final Path dirPath = new Path(rootPath, String.valueOf(index));
            return metaManager.createDirectory(getPathStr(dirPath));
          } catch (final Throwable t) {
            throw new RuntimeException(t);
          }
        }
      });
    }
    executorService.shutdown();

    // Verify that all operations succeeded
    int successCount = 0;
    for (final Future<Boolean> future : futures) {
      assertTrue("CreateDirectory should succeed", future.get());
      successCount += future.isCancelled() ? 0 : 1;
    }
    assertEquals(numDirs, successCount);

    for (int i = 0; i < numDirs; i++) {
      final int index = i;
      final Path filePath = new Path(getPathStr(rootPath), String.valueOf(index));

      // Metadata is loaded once while the directory is being created.
      final FileMeta result = metaManager.get(filePath, user);
      verify(metaLoader, times(1)).load(filePath);

      // Check validity of fileMeta
      assertNotNull(result);
      assertTrue("Should be a directory", result.isDirectory());
      assertEquals(getPathStr(filePath), result.getFullPath());
      // children-to-parent mapping
      assertEquals(rootFileMeta, metaManager.getParent(result));
      // parent-to-children mapping
      assertNotNull(metaManager.getChildren(rootFileMeta));
      assertTrue("Should be a child of rootPath, index: " + String.valueOf(index),
              metaManager.getChildren(rootFileMeta).contains(result));
    }
  }

  /**
   * Verify that clear properly clears the cache, and returns the number of
   * previously loaded paths.
   * @throws Throwable
   */
  @Test
  public void testClear() throws Throwable {
    final Path path = new Path("/path");
    final User user = defaultUser();

    final CacheLoader<Path, FileMeta> cacheLoader = mock(CacheLoader.class);
    final FileMeta fileMeta = new FileMeta();
    when(cacheLoader.load(path)).thenReturn(fileMeta);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater,
            blockMetaFactory, metaFactory, locationSorter, baseFsClient);
    when(cacheUpdater.updateMeta(eq(fileMeta))).thenReturn(fileMeta.deepCopy());

    assertEquals(0, metaManager.clear());
    metaManager.get(path, user);
    assertEquals(1, metaManager.clear());
    assertEquals(0, metaManager.clear());
  }

  private FileMeta fileMeta(final Path path, final long[] offsets, final String[] locations) {
    final FileMeta fileMeta = new FileMeta();
    fileMeta.setFullPath(path.toString());
    fileMeta.setBlockSize(blockSize);
    for (final long offset : offsets) {
      final BlockMeta blockMeta = new BlockMeta();
      fileMeta.addToBlocks(blockMeta);
      blockMeta.setFilePath(path.toString());
      blockMeta.setOffSet(offset);
      for (final String location : locations) {
        final NodeInfo nodeInfo = new NodeInfo();
        nodeInfo.setAddress(location);
        nodeInfo.setRack("/default");
        blockMeta.addToLocations(nodeInfo);
      }
    }
    return fileMeta;
  }

  private static void addRemoval(final CacheUpdates updates, final Path path, final long offset, final long blockSize) {
    final BlockId blockId = new BlockId(path.toString(), offset, blockSize);
    updates.addRemoval(blockId);
  }

  private static void addFailure(final CacheUpdates updates, final Path path, final long offset, final long blockSize) {
    final BlockId blockId = new BlockId(path.toString(), offset, blockSize);
    updates.addFailure(blockId, new IOException("Test"));
  }

  /**
   * Test that removal and failure updates are reflected in the cacheLocationRemover.
   */
  @Test
  public void testApplyUpdates() throws Throwable {
    final CacheLoader<Path, FileMeta> cacheLoader = mock(CacheLoader.class);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater,
            blockMetaFactory, metaFactory, locationSorter, baseFsClient);

    final String[] addresses = new String[]{ "localhost:17001", "localhost:17002", "localhost:17003" };
    final User user = defaultUser();

    final Path pathA = new Path("/path/fileA");
    {
      final long[] blockIds = new long[]{0, 1, 2, 3};
      final FileMeta fileMeta = fileMeta(pathA, blockIds, addresses);
      when(cacheLoader.load(pathA)).thenReturn(fileMeta);
      when(cacheUpdater.updateMeta(eq(fileMeta))).thenReturn(fileMeta.deepCopy());

      assertEquals(fileMeta, metaManager.get(pathA, user));
    }

    {
      final CacheUpdates updates = new CacheUpdates();
      addRemoval(updates, pathA, 0, blockSize);
      addRemoval(updates, pathA, 1, blockSize);
      addRemoval(updates, pathA, 2, blockSize);
      addRemoval(updates, pathA, 3, blockSize);
      final CacheNode cacheNode = mock(CacheNode.class);
      when(cacheNode.getAddress()).thenReturn(addresses[0]);
      metaManager.applyCacheNodeUpdates(cacheNode, updates);

      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathA.toString());
      assertEquals(4, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(1, pendingRemoves.get(blockId).size());
      }
    }

    {
      final CacheUpdates updates = new CacheUpdates();
      addFailure(updates, pathA, 0, blockSize);
      addFailure(updates, pathA, 1, blockSize);
      addRemoval(updates, pathA, 2, blockSize);
      addRemoval(updates, pathA, 3, blockSize);
      final CacheNode cacheNode = mock(CacheNode.class);
      when(cacheNode.getAddress()).thenReturn(addresses[1]);
      metaManager.applyCacheNodeUpdates(cacheNode, updates);

      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathA.toString());
      assertEquals(4, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(1, pendingRemoves.get(blockId).size());
      }
    }
  }

  /**
   * Test that removal and failure updates are reflected in the cacheLocationRemover,
   * under concurrent operations.
   */
  @Test
  public void testConcurrentUpdates() throws Throwable {
    final CacheLoader<Path, FileMeta> cacheLoader = mock(CacheLoader.class);
    final LoadingCacheConstructor constructor = new LoadingCacheConstructor(cacheLoader);
    final LoadingCache<Path, FileMeta> cache = constructor.newInstance();
    final SurfMetaManager metaManager = new SurfMetaManager(cache, cacheMessenger, cacheLocationRemover, cacheUpdater,
            blockMetaFactory, metaFactory, locationSorter, baseFsClient);

    final int numNodes = 10;
    int port = 17000;
    final String[] addresses = new String[numNodes];
    for (int i = 0; i < numNodes; i++) {
      addresses[i] = "localhost:" + (port++);
    }
    final User user = defaultUser();

    final int numBlocks = 200;
    final Path pathA = new Path("/path/fileA");
    {
      final long[] offsets = new long[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        offsets[i] = i;
      }
      final FileMeta fileMeta = fileMeta(pathA, offsets, addresses);
      when(cacheLoader.load(pathA)).thenReturn(fileMeta);
      when(cacheUpdater.updateMeta(eq(fileMeta))).thenReturn(fileMeta.deepCopy());

      assertEquals(fileMeta, metaManager.get(pathA, user));
    }

    final Path pathB = new Path("/path/fileB");
    {
      final long[] offsets = new long[numBlocks];
      for (int i = 0; i < numBlocks; i++) {
        offsets[i] = numBlocks + i;
      }
      final FileMeta fileMeta = fileMeta(pathB, offsets, addresses);
      when(cacheLoader.load(pathB)).thenReturn(fileMeta);
      when(cacheUpdater.updateMeta(eq(fileMeta))).thenReturn(fileMeta.deepCopy());
      assertEquals(fileMeta, metaManager.get(pathB, user));
    }

    final ExecutorService es = Executors.newFixedThreadPool(numNodes * 2);
    for (int i = 0; i < numNodes; i++) {
      final String address = addresses[i];
      es.submit(new Runnable() {
        @Override
        public void run() {
          final CacheUpdates updates = new CacheUpdates();
          for (int j = 0; j < numBlocks; j++) {
            addRemoval(updates, pathA, j, blockSize);
          }
          final CacheNode cacheNode = mock(CacheNode.class);
          when(cacheNode.getAddress()).thenReturn(address);
          metaManager.applyCacheNodeUpdates(cacheNode, updates);
        }
      });
      es.submit(new Runnable() {
        @Override
        public void run() {
          final CacheUpdates updates = new CacheUpdates();
          for (int j = 0; j < numBlocks; j++) {
            addRemoval(updates, pathB, numBlocks + j, blockSize);
          }
          final CacheNode cacheNode = mock(CacheNode.class);
          when(cacheNode.getAddress()).thenReturn(address);
          metaManager.applyCacheNodeUpdates(cacheNode, updates);
        }
      });
    }
    es.shutdown();
    final boolean terminated = es.awaitTermination(60, TimeUnit.SECONDS);
    assertTrue(terminated);

    {
      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathA.toString());
      assertEquals(numBlocks, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(10, pendingRemoves.get(blockId).size());
      }
    }

    {
      final Map<BlockId, List<String>> pendingRemoves = cacheLocationRemover.pullPendingRemoves(pathB.toString());
      assertEquals(numBlocks, pendingRemoves.size());
      for (final BlockId blockId : pendingRemoves.keySet()) {
        assertEquals(10, pendingRemoves.get(blockId).size());
      }
    }
  }

  private String getPathStr(final Path path) {
    return path.toUri().getPath();
  }
}
