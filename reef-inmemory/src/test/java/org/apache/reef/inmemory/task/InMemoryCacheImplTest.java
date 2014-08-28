package org.apache.reef.inmemory.task;

import com.microsoft.wake.EStage;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.CacheUpdates;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Tests for InMemoryCacheImpl
 */
public final class InMemoryCacheImplTest {

  private CacheStatistics statistics;
  private MemoryManager memoryManager;
  private EStage<BlockLoader> loadingStage;
  private InMemoryCache cache;
  private Random random = new Random();
  private static final long maxMemory = Runtime.getRuntime().maxMemory();

  @Before
  public void setUp() {
    final int slack = 100 * 1024 * 1024;
    final int bufferSize = 8 * 1024 * 1024;

    statistics = new CacheStatistics();
    memoryManager = new MemoryManager(statistics, slack);
    loadingStage = new MockStage(memoryManager);
    cache = new InMemoryCacheImpl(memoryManager, loadingStage, 3, bufferSize);
  }

  @After
  public void tearDown() {
    statistics = null;
    memoryManager = null;
    loadingStage = null;
    cache = null;

    System.gc();
  }

  private BlockId randomBlockId(long length) {
    return new MockBlockId(random.nextLong(), length);
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

  private byte[] randomOnesBuffer() {
    return ones(1 + random.nextInt(10));
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
  private void assertBlockLoaded(byte[] buffer, BlockId blockId) throws BlockLoadingException, BlockNotFoundException {
    int bufferSize = cache.getLoadingBufferSize();
    for(int i = 0; i * bufferSize < buffer.length; i++) {
      int startChunk = i * bufferSize;
      int endChunk = Math.min((i + 1) * bufferSize, buffer.length);
      assertArrayEquals(Arrays.copyOfRange(buffer, startChunk, endChunk), cache.get(blockId, i));
    }
  }

  /**
   * Put a small buffer in the task, and check correctness of get after load
   */
  @Test
  public void testPutAndGet() throws BlockLoadingException, BlockNotFoundException, IOException {
    final byte[] buffer = randomOnesBuffer();
    final BlockId blockId = randomBlockId(buffer.length);
    assertBlockNotFound(blockId, buffer.length);

    final BlockLoader loader = new MockBlockLoader(blockId, buffer, false, cache.getLoadingBufferSize());

    cache.load(loader, false);
    assertBlockLoaded(buffer, blockId);
  }

  /**
   * Put a small buffer in the task, and check correctness of clear
   */
  @Test
  public void testClear() throws BlockLoadingException, BlockNotFoundException, IOException {
    final byte[] buffer = randomOnesBuffer();
    final BlockId blockId = randomBlockId(buffer.length);

    final BlockLoader loader = new MockBlockLoader(blockId, buffer, false, cache.getLoadingBufferSize());

    cache.load(loader, false);
    assertBlockLoaded(buffer, blockId);
    cache.clear();
    assertBlockNotFound(blockId, buffer.length);

    assertEquals(0, statistics.getCacheBytes());
    assertEquals(0, statistics.getPinnedBytes());
    assertEquals(0, statistics.getEvictedBytes());
  }

  /**
   * Test statistics after load
   */
  @Test
  public void testStatistics() throws Exception {
    final byte[] buffer = randomOnesBuffer();
    final BlockId blockId = randomBlockId(buffer.length);

    final BlockLoader loader = new MockBlockLoader(blockId, buffer, false, cache.getLoadingBufferSize());

    cache.load(loader, false);
    assertEquals(buffer.length, cache.getStatistics().getCacheBytes());
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
    final BlockId blockId = randomBlockId(firstLoadBuffer.length);
    assertBlockNotFound(blockId, firstLoadBuffer.length);

    // Start long-running block load
    final BlockLoader firstLoader;
    {
      firstLoader = new SleepingBlockLoader(blockId, false, firstLoadBuffer, 3000, new AtomicInteger(0), cache.getLoadingBufferSize());
      e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            cache.load(firstLoader, false);
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
   * Start a cache.load() that takes 3 seconds to complete.
   * Then start multiple concurrent cache.load() on the same blockId.
   * Verify that only the first cache.load() succeeded, and none of the
   * subsequent load() calls resulted in a loader.loadBlock() invocation.
   */
  @Test
  public void testConcurrentLoads() throws Exception {
    final int numThreads = 10; // Must be > 3
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);

    final byte[] firstLoadBuffer = twos(1024);
    final BlockId blockId = randomBlockId(firstLoadBuffer.length);
    assertBlockNotFound(blockId, firstLoadBuffer.length);

    final BlockLoader[] loaders = new BlockLoader[numThreads];
    final Future<?>[] futures = new Future<?>[numThreads];
    final AtomicInteger[] loadCounts = new AtomicInteger[numThreads];
    for (int i = 0; i < numThreads; i++) {
      loadCounts[i] = new AtomicInteger(0);
    }

    // Start long-running block load
    {
      final BlockLoader firstLoader = new SleepingBlockLoader(blockId, false, firstLoadBuffer, 3000, loadCounts[0], cache.getLoadingBufferSize());

      loaders[0] = firstLoader;
      futures[0] = e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            cache.load(firstLoader, false);
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
      final BlockLoader loader = new MockBlockLoader(blockId, ones(1024), false, loadCounts[i], cache.getLoadingBufferSize());
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
          }

          // Then call load
          try {
            cache.load(loader, false);
          } catch (IOException e1) {
            fail("IOException "+e1);
          }
        }
      });
    }

    // Try to load different block
    for (int i = numThreads/2; i < numThreads; i++) {
      final byte[] randomBuffer = randomOnesBuffer();
      final BlockId randomId = randomBlockId(randomBuffer.length);
      // Load called on different blockId
      final BlockLoader loader = new MockBlockLoader(randomId, randomBuffer, false, loadCounts[i], cache.getLoadingBufferSize());
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
          }

          // Then call load
          try {
            cache.load(loader, false);
            assertBlockLoaded(randomBuffer, randomId);
          } catch (final IOException | BlockLoadingException | BlockNotFoundException e1) {
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

    // Check first block request loaded"
    assertBlockLoaded(firstLoadBuffer, blockId);

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
      }
    }
  }

  /**
   * Test that adding 20 * 128 MB = 2.5 GB of blocks does not cause an OutOfMemory exception.
   * Test that updates reflect the evictions.
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testEviction() throws IOException {
    final long iterations = maxMemory / (128 * 1024 * 1024) * 2;
    for (int i = 0; i < iterations; i++) {
      final byte[] buffer = ones(128 * 1024 * 1024);
      final BlockId blockId = randomBlockId(buffer.length);
      final BlockLoader loader = new MockBlockLoader(blockId, buffer, false, cache.getLoadingBufferSize());

      cache.load(loader, false);
      System.out.println("Loaded " + (128 * (i+1)) + "M");
    }

    final CacheUpdates updates = cache.pullUpdates();
    assertRemovalsNotFound(updates);
  }

  /**
   * Test that adding 20 * 128 MB = 2.5 GB of blocks concurrently does not cause an OutOfMemory exception.
   * Test that updates reflect the evictions.
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testConcurrentEviction() throws IOException, ExecutionException, InterruptedException {

    final int numThreads = 10;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);

    final int iterations = (int) (maxMemory / (128 * 1024 * 1024) * 2);

    final Future<?>[] futures = new Future<?>[iterations];

    for (int i = 0; i < iterations; i++) {
      final int iteration = i;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          final byte[] buffer = ones(128 * 1024 * 1024);
          final BlockId blockId = randomBlockId(buffer.length);
          final BlockLoader loader = new MockBlockLoader(blockId, buffer, false, cache.getLoadingBufferSize());

          try {
            cache.load(loader, false);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          System.out.println("Loaded " + (128 * (iteration + 1)) + "M");
        }
      });
    }

    for (int i = 0; i < iterations; i++) {
      futures[i].get();
    }

    assertTrue(true); // No exceptions were encountered

    final CacheUpdates updates = cache.pullUpdates();
    assertRemovalsNotFound(updates);
  }

  /**
   * Test that a pinned block remains, even after adding 20 * 128 MB = 2.5 GB of blocks
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testPinned() throws IOException, BlockLoadingException, BlockNotFoundException {
    // Different from other buffers, to tell them apart
    final byte[] pinnedBuffer = twos(128 * 1024 * 1024);
    final BlockId pinnedId = randomBlockId(pinnedBuffer.length);
    final int loadingBufferSize = cache.getLoadingBufferSize();
    final BlockLoader pinnedLoader = new MockBlockLoader(pinnedId, pinnedBuffer, true, loadingBufferSize);

    cache.load(pinnedLoader, true);
    assertBlockLoaded(pinnedBuffer, pinnedId);

    final long iterations = maxMemory / (128 * 1024 * 1024) * 2;
    for (int i = 0; i < iterations; i++) {
      final byte[] buffer = ones(128 * 1024 * 1024);
      final BlockId blockId = randomBlockId(buffer.length);

      final BlockLoader loader = new MockBlockLoader(blockId, buffer, false, loadingBufferSize);

      cache.load(loader, false);
      System.out.println("Loaded " + (128 * (i + 1)) + "M");
    }

    assertBlockLoaded(pinnedBuffer, pinnedId);
  }

  /**
   * Immediately load block, instead of using a stage
   */
  private static class MockStage implements EStage<BlockLoader> {

    private final BlockLoaderExecutor blockLoaderExecutor;

    public MockStage(final MemoryManager memoryManager) {
      this.blockLoaderExecutor = new BlockLoaderExecutor(memoryManager);
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
    private final boolean pinned;
    private final List<byte[]> data;
    private final int milliseconds;
    private boolean loadDone;
    private final AtomicInteger loadCount;
    private final int bufferSize;

    public SleepingBlockLoader(final BlockId blockId,
                               final boolean pin,
                               final byte[] data,
                               final int milliseconds,
                               final AtomicInteger loadCount,
                               final int bufferSize) {
      this.blockId = blockId;
      this.pinned = pin;
      this.milliseconds = milliseconds;
      this.loadCount = loadCount;
      this.loadDone = false;
      this.bufferSize = bufferSize;

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

    @Override
    public int getBufferSize() {
      return this.bufferSize;
    }
  }

  /**
   * Not using Mockito because Mockito causes OutOfMemory exception on memory-intensive tests
   */
  private static class MockBlockLoader implements BlockLoader {

    private final BlockId blockId;
    private final List<byte[]> data;
    private final boolean pinned;
    private final AtomicInteger loadCount;
    private final int bufferSize;

    public MockBlockLoader(final BlockId blockId, final byte[] data, final boolean pin, final int bufferSize) {
      this(blockId, data, pin, new AtomicInteger(0), bufferSize);
    }

    public MockBlockLoader(final BlockId blockId, final byte[] data, final boolean pin, final AtomicInteger loadCount, final int bufferSize) {
      this.blockId = blockId;
      this.pinned = pin;
      this.loadCount = loadCount;
      this.bufferSize = bufferSize;

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
    }

    @Override
    public BlockId getBlockId() {
      return blockId;
    }

    @Override
    public boolean isPinned() {
      return pinned;
    }

    @Override
    public byte[] getData(int index) throws BlockLoadingException {
      return data.get(index);
    }

    @Override
    public int getBufferSize() {
      return this.bufferSize;
    }
  }

  private static final class MockBlockId implements BlockId {

    private final long blockId;
    private final long blockSize;

    public MockBlockId(final long blockId,
                       final long blockSize) {
      this.blockId = blockId;
      this.blockSize = blockSize;
    }

    @Override
    public String getFilePath() {
      return "/mock/"+blockId;
    }

    @Override
    public long getOffset() {
      return 0;
    }

    @Override
    public long getUniqueId() {
      return blockId;
    }

    @Override
    public long getBlockSize() {
      return blockSize;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (o == null || getClass() != o.getClass()) return false;

      MockBlockId that = (MockBlockId) o;

      if (blockId != that.blockId) return false;
      if (blockSize != that.blockSize) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = (int) (blockId ^ (blockId >>> 32));
      result = 31 * result + (int) (blockSize ^ (blockSize >>> 32));
      return result;
    }
  }
}