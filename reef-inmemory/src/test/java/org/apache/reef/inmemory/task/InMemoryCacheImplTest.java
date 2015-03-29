package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.exceptions.BlockNotWritableException;
import org.apache.reef.inmemory.common.exceptions.BlockWritingException;
import org.apache.reef.task.HeartBeatTriggerManager;
import org.apache.reef.wake.EStage;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for InMemoryCacheImpl
 */
public final class InMemoryCacheImplTest {

  private CacheStatistics statistics;
  private Cache<BlockId, CacheEntry> internalCache;
  private MemoryManager memoryManager;
  private EStage<BlockLoader> loadingStage;
  private HeartBeatTriggerManager heartBeatTriggerManager;
  private CacheEntryFactory cacheEntryFactory;
  private InMemoryCache cache;
  private static final Random random = new Random();
  private static final double slack = 0.1;
  private final int bufferSize = 8 * 1024 * 1024;

  @Before
  public void setUp() {
    statistics = new CacheStatistics();
    final LRUEvictionManager lruEvictionManager = new LRUEvictionManager();
    memoryManager = new MemoryManager(lruEvictionManager, statistics, slack);
    final CacheConstructor cacheConstructor = new CacheConstructor(memoryManager, 5);
    internalCache = cacheConstructor.newInstance();
    final CacheAdmissionController cacheAdmissionController = new CacheAdmissionController(memoryManager, internalCache);
    loadingStage = new MockStage(cacheAdmissionController, memoryManager);
    heartBeatTriggerManager = mock(HeartBeatTriggerManager.class);
    cacheEntryFactory = new CacheEntryFactory();
    cache = new InMemoryCacheImpl(internalCache, memoryManager, cacheAdmissionController, lruEvictionManager,
            loadingStage, 3, bufferSize, heartBeatTriggerManager, cacheEntryFactory);
  }

  @After
  public void tearDown() throws IOException {
    statistics = null;
    internalCache = null;
    memoryManager = null;
    loadingStage = null;
    cache = null;

    System.gc();
  }

  private BlockId randomBlockId() {
    return new BlockId("/path", random.nextLong());
  }

  private byte[] ones(int length) {
    byte[] buf = new byte[length];
    Arrays.fill(buf, (byte) 1);
    return buf;
  }

  private byte[] twos(int length) {
    byte[] buf = new byte[length];
    Arrays.fill(buf, (byte) 2);
    return buf;
  }

  /**
   * Assert that Block is not found. Test with all the chunks in the block.
   */
  private void assertBlockNotFound(BlockId blockId, int bufferLength) {
    int bufferSize = cache.getLoadingBufferSize();
    for(int i = 0; i * bufferSize < bufferLength; i++) {
      try {
        cache.get(blockId, i);
        fail("Should have thrown BlockNotFoundException");
      } catch (BlockNotFoundException e) {
        assertTrue(true);
      } catch (Exception e) {
        fail("Should have thrown BlockNotFoundException");
      }
    }
  }

  /**
   * Assert that Block is loaded successfully. Test with all the chunks in the block.
   */
  private void assertBlockLoaded(byte[] buffer, BlockId blockId)
          throws BlockLoadingException, BlockNotFoundException, BlockWritingException {
    int bufferSize = cache.getLoadingBufferSize();
    for(int i = 0; i * bufferSize < buffer.length; i++) {
      int startChunk = i * bufferSize;
      int endChunk = Math.min((i + 1) * bufferSize, buffer.length);
      assertArrayEquals(Arrays.copyOfRange(buffer, startChunk, endChunk), cache.get(blockId, i));
    }
  }

  /**
   * Assert that Block is loaded successfully. Test with all the chunks in the block.
   * Loading provided should be same passed in to cache.
   */
  private void assertBlockLoaded(BlockLoader loader, BlockId blockId)
          throws BlockLoadingException, BlockNotFoundException, BlockWritingException {
    for (int i = 0; i * cache.getLoadingBufferSize() < loader.getBlockSize(); i++) {
      assertNotNull(cache.get(blockId, i));
      assertEquals(loader.getData(i), cache.get(blockId, i));
    }
  }

  /**
   * Ask cache to prepare for a blockLoader, and check correctness of preparation
   * {@code cache.prepareToWrite} marks the blockLoader as on the loading stage.
   */
  @Test
  public void testPrepare() throws IOException, BlockNotFoundException {
    final BlockId blockId = randomBlockId();
    final int blockSize = 8096;

    assertBlockNotFound(blockId, blockSize);

    final BlockReceiver receiver = mock(BlockReceiver.class);
    when(receiver.getBlockId()).thenReturn(blockId);
    when(receiver.getBlockSize()).thenReturn((long) blockSize);
    cache.clear();
    cache.prepareToWrite(receiver);

    assertEquals(0, statistics.getCacheBytes());
    assertEquals(blockSize, statistics.getLoadingBytes());
    assertEquals(0, statistics.getEvictedBytes());
    assertEquals(0, statistics.getPinnedBytes());
  }

  /**
   * Put a small buffer in the task, and check correctness of get after load
   */
  @Test
  public void testPutAndGet() throws BlockLoadingException, BlockNotFoundException, IOException, BlockWritingException {
    final BlockId blockId = randomBlockId();
    final int blockSize = 8096;

    assertBlockNotFound(blockId, blockSize);

    final BlockLoader loader = new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(blockSize), false);

    cache.load(loader);
    assertBlockLoaded(loader, blockId);
  }

  /**
   * Fill the data as large as the block size.
   * Block Size : 1024 / Buffer size : 128 / Packet size : 32
   * @throws IOException
   * @throws BlockNotFoundException
   * @throws BlockNotWritableException
   */
  @Test
  public void testWriteFullBlock() throws IOException, BlockNotFoundException, BlockNotWritableException, BlockLoadingException, BlockWritingException {
    final String fileName = "/write";
    final int offset = 0;
    final int blockSize = 1024;
    final int bufferSize = 128;
    final int packetSize = 32;

    final byte[] data = new byte[blockSize];
    new Random().nextBytes(data);

    final BlockId blockId = new BlockId(fileName, offset);
    final BlockReceiver blockReceiver = new MockBlockReceiver(blockId, blockSize, bufferSize, false);

    cache.prepareToWrite(blockReceiver);

    for (int packetIndex = 0; packetIndex < blockSize / packetSize; packetIndex++) {
      byte[] packet = new byte[packetSize];
      System.arraycopy(data, packetIndex * packetSize, packet, 0, packetSize);
      boolean isLastPacket = (packetIndex == blockSize / packetSize - 1);
      System.out.println("last? "+isLastPacket+" i "+packetIndex);
      cache.write(blockId, packetIndex * packetSize, ByteBuffer.wrap(packet), isLastPacket);
    }

    // Check the status is updated correctly.
    assertEquals(blockSize, statistics.getCacheBytes());
    assertEquals(0, statistics.getLoadingBytes());
    assertEquals(0, statistics.getEvictedBytes());
    assertEquals(0, statistics.getPinnedBytes());

    // Collect the loaded buffers and compare to the original data.
    ByteBuffer loaded = ByteBuffer.allocate(blockSize);
    for (int bufferIndex = 0; bufferIndex < blockSize / bufferSize; bufferIndex++) {
      loaded.put(cache.get(blockId, bufferIndex));
    }
    assertArrayEquals(data, loaded.array());
  }

  /**
   * Put a small buffer in the task, and check correctness of clear
   */
  @Test
  public void testClear() throws BlockLoadingException, BlockNotFoundException, IOException, BlockWritingException {
    final BlockId blockId = randomBlockId();
    final int blockSize = 8096;

    final BlockLoader loader = new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(blockSize), false);

    cache.load(loader);
    assertBlockLoaded(loader, blockId);
    cache.clear();
    assertBlockNotFound(blockId, blockSize);

    assertEquals(0, statistics.getCacheBytes());
    assertEquals(0, statistics.getPinnedBytes());
    assertEquals(0, statistics.getEvictedBytes());
  }

  /**
   * Test statistics after load
   */
  @Test
  public void testStatistics() throws Exception {
    final BlockId blockId = randomBlockId();
    final int blockSize = 8096;

    final BlockLoader loader = new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(blockSize), false);

    cache.load(loader);
    assertEquals(blockSize, cache.getStatistics().getCacheBytes());
    assertEquals(0, cache.getStatistics().getLoadingBytes());

    cache.clear();
    assertEquals(0, cache.getStatistics().getCacheBytes());
    assertEquals(0, cache.getStatistics().getLoadingBytes());
  }

  /**
   * Test statistics during load
   */
  @Test
  public void testStatisticsDuringLoad() throws InterruptedException {
    final int numThreads = 2;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);

    final byte[] firstLoadBuffer = twos(1024);
    final long blockSize = firstLoadBuffer.length;
    final BlockId blockId = randomBlockId();
    assertBlockNotFound(blockId, firstLoadBuffer.length);

    // Start long-running block load
    final BlockLoader firstLoader;
    {
      firstLoader = new SleepingBlockLoader(blockId, blockSize, false, firstLoadBuffer, 3000, new AtomicInteger(0), cache.getLoadingBufferSize());
      e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            cache.load(firstLoader);
          } catch (IOException e1) {
            fail("IOException " + e1);
          }
        }
      });
    }

    Thread.sleep(500); // Allow first block load to start

    assertEquals(1024, cache.getStatistics().getLoadingBytes());
    assertEquals(0, cache.getStatistics().getCacheBytes());
    assertEquals(0, cache.getStatistics().getPinnedBytes());
    assertEquals(0, cache.getStatistics().getEvictedBytes());
  }

  /**
   * Tests for correct cache behavior on concurrent load calls to the same blockId.
   *
   * Start a cache.load() that takes 5 seconds to complete.
   * Then start multiple concurrent cache.load() on the same blockId.
   * Verify that only the first cache.load() succeeded, and none of the
   * subsequent load() calls resulted in a loader.loadBlock() invocation.
   */
  @Test
  public void testConcurrentLoads() throws Exception {
    final int numThreads = 10; // Must be > 3
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);

    final byte[] firstLoadBuffer = twos(1024);
    final long blockSize = firstLoadBuffer.length;
    final BlockId blockId = randomBlockId();
    assertBlockNotFound(blockId, 1024);

    final BlockLoader[] loaders = new BlockLoader[numThreads];
    final Future<?>[] futures = new Future<?>[numThreads];
    final AtomicInteger[] loadCounts = new AtomicInteger[numThreads];
    for (int i = 0; i < numThreads; i++) {
      loadCounts[i] = new AtomicInteger(0);
    }

    // Start long-running block load
    final BlockLoader firstLoader = new SleepingBlockLoader(blockId, blockSize, false, firstLoadBuffer, 3000, loadCounts[0], 1024);
    {
      loaders[0] = firstLoader;
      futures[0] = e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            cache.load(firstLoader);
          } catch (IOException e1) {
            fail("IOException " + e1);
          }
        }
      });
    }

    Thread.sleep(500); // Allow first block load to start

    // Try to load same block
    for (int i = 1; i < numThreads/2; i++) {
      // Load called on same blockId
      final BlockLoader loader = new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(1024), false, loadCounts[i]);
      loaders[i] = loader;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          // First test if there is a block loading exception as expected
          try {
            cache.get(blockId, 0);
            fail("Expected BlockLoadingException but did not get an exception");
          } catch (BlockLoadingException e1) {
            // Expected
          } catch (BlockNotFoundException e1) {
            fail("Expected BlockLoadingException but received BlockNotFoundException");
          } catch (BlockWritingException e1) {
            fail("Expected BlockLoadingException but received BlockWritingException");
          }

          // Then call load
          try {
            cache.load(loader);
          } catch (IOException e1) {
            fail("IOException " + e1);
          }
        }
      });
    }

    // Try to load different block
    for (int i = numThreads/2; i < numThreads; i++) {
      final BlockId randomId = randomBlockId();
      // Load called on different blockId
      final BlockLoader loader = new MockBlockLoader(randomId, blockSize, new TwosBufferLoader(1024), false, loadCounts[i]);
      loaders[i] = loader;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          // First test if there is a block not found exception as expected
          try {
            cache.get(randomId, 0);
            fail("Expected BlockNotFoundException but did not get an exception");
          } catch (BlockLoadingException e1) {
            fail("Expected BlockNotFoundException but received BlockLoadingException");
          } catch (BlockNotFoundException e1) {
            // Expected
          } catch (BlockWritingException e1) {
            fail("Expected BlockNotFoundException but received BlockWritingException");
          }

          // Then call load
          try {
            cache.load(loader);
            assertBlockLoaded(loader, randomId);
          } catch (final IOException | BlockLoadingException | BlockNotFoundException | BlockWritingException e1) {
            fail("Exception " + e1);
          }
        }
      });
    }

    e.shutdown();
    e.awaitTermination(10, TimeUnit.SECONDS);

    for (int i = 0; i < numThreads; i++) {
      futures[i].get();
    }

    assertBlockLoaded(firstLoader, blockId);

    // Verify first loader called once
    assertEquals(1, loadCounts[0].get());

    // Verify loaders for same blockId not called
    for (int i = 1; i < numThreads/2; i++) {
      assertEquals(0, loadCounts[i].get());
    }

    // Verify loaders for different blockId called once
    for (int i = numThreads/2; i < numThreads; i++) {
      assertEquals(1, loadCounts[i].get());
    }
  }

  private void assertRemovalsNotFound(final CacheUpdates updates) {
    assertTrue(updates.getRemovals().size() > 0);
    for (final BlockId blockId : updates.getRemovals()) {
      try {
        cache.get(blockId, 0);
        assertTrue("BlockNotFoundException was excepted, but no exception was raised", false);
      } catch (BlockNotFoundException e) {
        assertTrue("BlockNotFoundException, as expected", true);
      } catch (BlockLoadingException e) {
        assertTrue("BlockNotFoundException was expected, but BlockLoadingException was raised", false);
      } catch (BlockWritingException e) {
        assertTrue("BlockNotFoundException was expected, but BlockWritingException was raised", false);
      }
    }
  }

  /**
   * Test that adding 50 * 128 MB = 6 GB of blocks does not cause an OutOfMemory exception.
   * Test that updates reflect the evictions.
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testEviction() throws Exception {
    final int blockSize = 128 * 1024 * 1024;
    final long iterations = 50;
    for (int i = 0; i < iterations; i++) {
      final BlockId blockId = randomBlockId();
      final BlockLoader loader = new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(blockSize), false);

      cache.load(loader);
      System.out.println("Loaded " + (blockSize / 1024 / 1024 * (i + 1)) + "M");
      System.out.println("Statistics: " + cache.getStatistics());
      Thread.sleep(10);
    }

    System.out.println("Statistics: " + cache.getStatistics());

    final long usableCache = (long) (cache.getStatistics().getMaxBytes() * (1.0 - slack));
    final long expectedCached = usableCache - (usableCache % blockSize);
    final long expectedEvicted = (blockSize * iterations) - expectedCached;

    assertEquals(expectedCached, statistics.getCacheBytes());
    assertEquals(expectedEvicted, statistics.getEvictedBytes());
  }

  /**
   * Test that adding 50 * 128 MB = 6 GB of blocks concurrently does not cause an OutOfMemory exception
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testConcurrentEviction() throws IOException, ExecutionException, InterruptedException {
    final int blockSize = 128 * 1024 * 1024;

    final int numThreads = 10;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);

    final long iterations = 50;

    final Future<?>[] futures = new Future<?>[(int)iterations];

    for (int i = 0; i < iterations; i++) {
      final int iteration = i;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          final BlockId blockId = randomBlockId();
          final BlockLoader loader = new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(blockSize), false);

          try {
            cache.load(loader);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          System.out.println("Loaded " + (blockSize / 1024 / 1024 * (iteration + 1)) + "M");
          System.out.println("Statistics: " + cache.getStatistics());
        }
      });
    }

    for (int i = 0; i < iterations; i++) {
      futures[i].get();
    }

    System.out.println("Statistics: " + cache.getStatistics());

    final long usableCache = (long) (cache.getStatistics().getMaxBytes() * (1.0 - slack));
    final long expectedCached = usableCache - (usableCache % blockSize);
    final long expectedEvicted = (blockSize * iterations) - expectedCached;

    assertEquals(expectedCached, statistics.getCacheBytes());
    assertEquals(expectedEvicted, statistics.getEvictedBytes());
  }

  /**
   * Test that running a clear() while concurrently loading does not cause errors
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testConcurrentClear() throws IOException, ExecutionException, InterruptedException {
    final int blockSize = 128 * 1024 * 1024;

    final int numThreads = 10;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);

    final long iterations = 50;

    final Future<?>[] futures = new Future<?>[(int)iterations];

    for (int i = 0; i < iterations; i++) {
      final int iteration = i;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          final BlockId blockId = randomBlockId();
          final BlockLoader loader = new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(blockSize), false);

          if (iteration == 9 || iteration == 10) {
            cache.clear();
          }

          try {
            cache.load(loader);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          System.out.println("Loaded " + (blockSize / 1024 / 1024 * (iteration + 1)) + "M");
          System.out.println("Statistics: " + cache.getStatistics());
        }
      });
    }

    for (int i = 0; i < iterations; i++) {
      futures[i].get();
    }

    System.out.println("Statistics: " + cache.getStatistics());

    final long usableCache = (long) (cache.getStatistics().getMaxBytes() * (1.0 - slack));
    final long expectedCached = usableCache - (usableCache % blockSize);
    final long maxEvicted = (blockSize * iterations) - expectedCached;

    assertEquals(expectedCached, statistics.getCacheBytes());
    assertTrue(statistics.getEvictedBytes() + " of at most " + maxEvicted + " were evicted",
            maxEvicted >= statistics.getEvictedBytes());
  }

  /**
   * Test that a pinned block remains, even after adding 20 * 128 MB = 2.5 GB of blocks
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testPinned() throws IOException, BlockLoadingException, BlockNotFoundException, BlockWritingException {
    final int blockSize = 128 * 1024 * 1024;
    // Different from other buffers, to tell them apart
    final BlockId pinnedId = randomBlockId();

    final BlockLoader pinnedLoader = new MockBlockLoader(pinnedId, blockSize, new TwosBufferLoader(blockSize), true);

    cache.load(pinnedLoader);
    assertBlockLoaded(pinnedLoader, pinnedId);

    final long iterations = 50;
    for (int i = 0; i < iterations; i++) {
      final BlockId blockId = randomBlockId();

      final BlockLoader loader = new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(blockSize), false);

      cache.load(loader);
      System.out.println("Loaded " + (128 * (i + 1)) + "M");
    }

    assertBlockLoaded(pinnedLoader, pinnedId);
  }

  @Test
  public void testTooManyPinned() throws Exception {
    {
      final CacheUpdates updates = cache.pullUpdates();
      assertEquals(0, updates.getFailures().size());
      assertEquals(0, updates.getRemovals().size());
    }

    final long iterations = 50;
    for (int i = 0; i < iterations; i++) {
      final long blockSize = 128 * 1024 * 1024;
      final BlockId blockId = randomBlockId();
      BlockLoader loader =
              new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(128 * 1024 * 1024), true);

      cache.load(loader);
      System.out.println("Loaded " + (128 * (i + 1)) + "M");
      loader = null;

      System.out.println("Max Memory: " + Runtime.getRuntime().maxMemory());
      System.out.println("Free Memory: " + Runtime.getRuntime().freeMemory());
      System.out.println("Total Memory: " + Runtime.getRuntime().totalMemory());
    }

    {
      final CacheUpdates updates = cache.pullUpdates();
      assertTrue("Failures exist", updates.getFailures().size() > 0);
      assertEquals(0, updates.getRemovals().size());
    }
  }

  /**
   * Test that adding 20 * 128 MB = 2.5 GB of blocks concurrently does not cause an OutOfMemory exception
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testConcurrentPinnedAndEviction() throws IOException, ExecutionException, InterruptedException {
    final int blockSize = 128 * 1024 * 1024;

    final int numThreads = 10;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);

    final int iterations = 50;

    final Future<?>[] futures = new Future<?>[iterations];
    final AtomicInteger[] loadCounts = new AtomicInteger[3];

    for (int i = 0; i < iterations; i++) {
      final int iteration = i;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          final BlockId blockId = randomBlockId();
          final boolean pin = (iteration % 20 == 3); // 3, 23, 43
          final AtomicInteger loadCount = new AtomicInteger(0);
          final BlockLoader loader =
                  new MockBlockLoader(blockId, blockSize, new OnesBufferLoader(blockSize), pin, loadCount);
          if (pin) {
            loadCounts[iteration/20] = loadCount;
          }
          try {
            cache.load(loader);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          System.out.println("Loaded " + (blockSize / 1024 / 1024 * (iteration + 1)) + "M");
          System.out.println("Statistics: " + cache.getStatistics());
        }
      });
    }

    for (int i = 0; i < iterations; i++) {
      futures[i].get();
    }

    System.out.println("Statistics: " + cache.getStatistics());
    assertEquals(3 * blockSize, statistics.getPinnedBytes());
    assertEquals(1, loadCounts[0].get());
    assertEquals(1, loadCounts[1].get());
    assertEquals(1, loadCounts[2].get());
    assertEquals((long) (iterations - 3) * blockSize, statistics.getCacheBytes() + statistics.getEvictedBytes());
    assertEquals(3 * blockSize, statistics.getPinnedBytes());
  }

  /**
   * Immediately load block, instead of using a stage
   */
  private static class MockStage implements EStage<BlockLoader> {

    private final BlockLoaderExecutor blockLoaderExecutor;

    public MockStage(final CacheAdmissionController cacheAdmissionController,
                     final MemoryManager memoryManager) {
      this.blockLoaderExecutor = new BlockLoaderExecutor(cacheAdmissionController, memoryManager);
    }

    @Override
    public void close() throws Exception {
    }

    @Override
    public void onNext(BlockLoader loader) {
      blockLoaderExecutor.onNext(loader);
    }
  }

  private static class SleepingBlockLoader implements BlockLoader {

    private final BlockId blockId;
    private final long blockSize;
    private final boolean pinned;
    private final List<byte[]> data;
    private final int milliseconds;
    private boolean loadDone;
    private final AtomicInteger loadCount;

    public SleepingBlockLoader(final BlockId blockId,
                               final long blockSize,
                               final boolean pin,
                               final byte[] data,
                               final int milliseconds,
                               final AtomicInteger loadCount,
                               final int bufferSize) {
      this.blockId = blockId;
      this.blockSize = blockSize;
      this.pinned = pin;
      this.milliseconds = milliseconds;
      this.loadCount = loadCount;
      this.loadDone = false;

      // Load data chunks from the byte array
      ArrayList<byte[]> temp = new ArrayList<>();
      for(int i = 0; i * bufferSize < data.length; i++) {
        int startChunk = i * bufferSize;
        int endChunk = Math.min((i + 1) * bufferSize, data.length);
        temp.add(Arrays.copyOfRange(data, startChunk, endChunk));
      }
      this.data = temp;
    }

    @Override
    public void loadBlock() throws IOException {
      loadCount.incrementAndGet();
      System.out.println("Start load while sleeping");

      try {
        Thread.sleep(milliseconds);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }

      loadDone = true;
      System.out.println("Done load while sleeping");
    }

    @Override
    public BlockId getBlockId() {
      return blockId;
    }

    @Override
    public long getBlockSize() {
      return blockSize;
    }

    @Override
    public boolean isPinned() {
      return pinned;
    }

    @Override
    public byte[] getData(int index) throws BlockLoadingException {
      if (loadDone) {
        return data.get(index);
      } else {
        throw new BlockLoadingException();
      }
    }
  }

  private interface BufferLoader {
    public List<byte[]> loadBuffer();
  }

  private final class OnesBufferLoader implements BufferLoader {
    private final int length;

    public OnesBufferLoader(final int length) {
      this.length = length;
    }

    @Override
    public List<byte[]> loadBuffer() {
      final List<byte[]> buffer = new LinkedList<>();
      long remaining = length;
      while(remaining > 0) {
        final int writeSize = remaining < bufferSize ? (int)remaining : bufferSize;
        buffer.add(ones(writeSize));
        remaining -= writeSize;
      }
      return buffer;
    }
  }

  private final class TwosBufferLoader implements BufferLoader {
    private final int length;

    public TwosBufferLoader(final int length) {
      this.length = length;
    }

    @Override
    public List<byte[]> loadBuffer() {
      List<byte[]> buffer = new LinkedList<>();
      long remaining = length;
      while(remaining > 0) {
        final int writeSize = remaining < bufferSize ? (int)remaining : bufferSize;
        buffer.add(twos(writeSize));
        remaining -= writeSize;
      }
      return buffer;
    }
  }

  /**
   * Not using Mockito because Mockito causes OutOfMemory exception on memory-intensive tests
   */
  private static class MockBlockLoader implements BlockLoader {

    private final BlockId blockId;
    private final long blockSize;
    private final BufferLoader bufferLoader;
    private final boolean pinned;
    private final AtomicInteger loadCount;
    private List<byte[]> data;

    public MockBlockLoader(final BlockId blockId, final long blockSize, final BufferLoader bufferLoader, final boolean pin) {
      this(blockId, blockSize, bufferLoader, pin, new AtomicInteger(0));
    }

    public MockBlockLoader(final BlockId blockId,
                           final long blockSize,
                           final BufferLoader bufferLoader,
                           final boolean pin,
                           final AtomicInteger loadCount) {
      this.blockId = blockId;
      this.blockSize = blockSize;
      this.bufferLoader = bufferLoader;
      this.pinned = pin;
      this.loadCount = loadCount;
    }

    @Override
    public void loadBlock() throws IOException {
      data = bufferLoader.loadBuffer();
      loadCount.incrementAndGet();
    }

    @Override
    public BlockId getBlockId() {
      return blockId;
    }

    @Override
    public long getBlockSize() {
      return blockSize;
    }

    @Override
    public boolean isPinned() {
      return pinned;
    }

    @Override
    public byte[] getData(int index) throws BlockLoadingException {
      return data.get(index);
    }
  }

  // TODO This class is copied from HdfsBlockReceiver implementation. Make it simple for the test.
  private static class MockBlockReceiver implements BlockReceiver {
    private BlockId blockId;
    private long blockSize;
    private boolean pin;
    private int bufferSize;
    private boolean isComplete = false;
    private long totalWritten = 0;
    private long expectedOffset = 0;
    private final List<ByteBuffer> data;

    public MockBlockReceiver(final BlockId blockId,
                             final long blockSize,
                             final int bufferSize,
                             final boolean pin) {
      this.blockId = blockId;
      this.blockSize = blockSize;
      this.bufferSize = bufferSize;
      this.pin = pin;

      this.data = new ArrayList<>();
    }

    @Override
    public BlockId getBlockId() {
      return blockId;
    }

    @Override
    public long getBlockSize() {
      return blockSize;
    }

    @Override
    public boolean isPinned() {
      return pin;
    }

    @Override
    public byte[] getData(int index) throws BlockWritingException {
      // If the date is not completely written for this block, throw BlockLoadingException.
      if (!isComplete || index >= data.size()) {
        throw new BlockWritingException(totalWritten);
      }

      final ByteBuffer buf = this.data.get(index);
      if(buf.position() != bufferSize) {
        final byte[] bArray = new byte[buf.position()];
        buf.position(0);
        buf.get(bArray);
        return bArray;
      } else {
        return buf.array();
      }
    }

    @Override
    public void writeData(byte[] data, long offset) throws IOException {
      if (offset + data.length > blockSize) {
        throw new IOException("The data exceeds the capacity of block. Offset : " + offset
                + " , Packet length : " + data.length + " Block size : " + blockSize);
      } else if (!isValidOffset(offset)) {
        throw new IOException("Received packet with an invalid offset " + offset);
      }

      int index = (int) (offset / bufferSize);
      int innerOffset = (int) (offset % bufferSize);
      int nWritten = 0;

      while (nWritten < data.length) {
        final ByteBuffer buf = getBuffer(index);
        final int toWrite = Math.min(bufferSize - innerOffset, data.length - nWritten);
        buf.put(data, nWritten, toWrite);

        index++;
        innerOffset = 0;
        nWritten += toWrite;
      }

      totalWritten += nWritten;
      updateValidOffset(offset, data.length);

    }

    @Override
    public void completeWrite() {
      isComplete = true;
    }

    @Override
    public long getTotalWritten() {
      return totalWritten;
    }

    /**
     * Get the buffer to write data into. If the buffer for the range
     * does not exist, create one and put in the cache on demand.
     * @param index The index of buffer stored in the cache
     * @return The byte buffer
     */
    private synchronized ByteBuffer getBuffer(final int index) {
      if (index >= data.size()) {
        // If blockSize is smaller than blockSize, then the blockSize will cover the whole data
        final ByteBuffer buf = ByteBuffer.allocate(Math.min(bufferSize, (int)blockSize));
        data.add(buf);
      }
      return data.get(index);
    }

    /**
     * Update the valid offsets.
     * @param previousOffset The valid offset for the previous packet.
     * @param packetLength The length of packet received.
     */
    private void updateValidOffset(final long previousOffset, final int packetLength) {
      expectedOffset = previousOffset + packetLength;
    }

    /**
     * Determine offset of the packet is valid in order to avoid duplicate or miss.
     * The assumption is the packets always come in-order.
     * @param offset The offset of the packet
     * @return {@code true} if the offset is valid to receive
     */
    private boolean isValidOffset(final long offset) {
      return expectedOffset == offset;
    }
  }
}
