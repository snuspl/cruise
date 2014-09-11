package org.apache.reef.inmemory.task;

import com.google.common.cache.Cache;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;

/**
 * Tests for InMemoryCacheImpl
 */
public final class InMemoryCacheImplTest {

  private CacheStatistics statistics;
  private Cache<BlockId, BlockLoader> internalCache;
  private MemoryManager memoryManager;
  private EStage<BlockLoader> loadingStage;
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
    loadingStage = new MockStage(internalCache, memoryManager);
    cache = new InMemoryCacheImpl(internalCache, memoryManager, lruEvictionManager, loadingStage, 3, bufferSize);
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
   * Assert that Block is loaded successfully. Test with all the chunks in the block.
   * Loading provided should be same passed in to cache.
   */
  private void assertBlockLoaded(BlockLoader loader, BlockId blockId) throws BlockLoadingException, BlockNotFoundException {
    for (int i = 0; i * loader.getBufferSize() < blockId.getBlockSize(); i++) {
      assertNotNull(cache.get(blockId, i));
      assertEquals(loader.getData(i), cache.get(blockId, i));
    }
  }

  /**
   * Put a small buffer in the task, and check correctness of get after load
   */
  @Test
  public void testPutAndGet() throws BlockLoadingException, BlockNotFoundException, IOException {
    final BlockId blockId = randomBlockId(8096);
    assertBlockNotFound(blockId, 8096);

    final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(8096), false);

    cache.load(loader);
    assertBlockLoaded(loader, blockId);
  }

  /**
   * Put a small buffer in the task, and check correctness of clear
   */
  @Test
  public void testClear() throws BlockLoadingException, BlockNotFoundException, IOException {
    final BlockId blockId = randomBlockId(1024);

    final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(8096), false);

    cache.load(loader);
    assertBlockLoaded(loader, blockId);
    cache.clear();
    assertBlockNotFound(blockId, 1024);

    assertEquals(0, statistics.getCacheBytes());
    assertEquals(0, statistics.getPinnedBytes());
    assertEquals(0, statistics.getEvictedBytes());
  }

  /**
   * Test statistics after load
   */
  @Test
  public void testStatistics() throws Exception {
    final BlockId blockId = randomBlockId(8096);

    final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(8096), false);

    cache.load(loader);
    assertEquals(8096, cache.getStatistics().getCacheBytes());
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
    final BlockId blockId = randomBlockId(firstLoadBuffer.length);
    assertBlockNotFound(blockId, 1024);

    final BlockLoader[] loaders = new BlockLoader[numThreads];
    final Future<?>[] futures = new Future<?>[numThreads];
    final AtomicInteger[] loadCounts = new AtomicInteger[numThreads];
    for (int i = 0; i < numThreads; i++) {
      loadCounts[i] = new AtomicInteger(0);
    }

    // Start long-running block load
    final BlockLoader firstLoader = new SleepingBlockLoader(blockId, false, firstLoadBuffer, 3000, loadCounts[0], 1024);
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
      final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(1024), false, loadCounts[i], 1024);
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
            cache.load(loader);
          } catch (IOException e1) {
            fail("IOException "+e1);
          }
        }
      });
    }

    // Try to load different block
    for (int i = numThreads/2; i < numThreads; i++) {
      final BlockId randomId = randomBlockId(1024);
      // Load called on different blockId
      final BlockLoader loader = new MockBlockLoader(randomId, new TwosBufferLoader(1024), false, loadCounts[i], 1024);
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
            cache.load(loader);
            assertBlockLoaded(loader, randomId);
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
      final BlockId blockId = randomBlockId(blockSize);
      final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(blockSize), false);

      cache.load(loader);
      System.out.println("Loaded " + (blockSize / 1024 / 1024 * (i+1)) + "M");
      System.out.println("Statistics: "+cache.getStatistics());
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
          final BlockId blockId = randomBlockId(blockSize);
          final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(blockSize), false);

          try {
            cache.load(loader);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          System.out.println("Loaded " + (blockSize / 1024 / 1024 * (iteration + 1)) + "M");
          System.out.println("Statistics: "+cache.getStatistics());
        }
      });
    }

    for (int i = 0; i < iterations; i++) {
      futures[i].get();
    }

    System.out.println("Statistics: "+cache.getStatistics());

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
          final BlockId blockId = randomBlockId(blockSize);
          final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(blockSize), false);

          if (iteration == 9 || iteration == 10) {
            cache.clear();
          }

          try {
            cache.load(loader);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          System.out.println("Loaded " + (blockSize / 1024 / 1024 * (iteration + 1)) + "M");
          System.out.println("Statistics: "+cache.getStatistics());
        }
      });
    }

    for (int i = 0; i < iterations; i++) {
      futures[i].get();
    }

    System.out.println("Statistics: "+cache.getStatistics());

    final long usableCache = (long) (cache.getStatistics().getMaxBytes() * (1.0 - slack));
    final long expectedCached = usableCache - (usableCache % blockSize);
    final long maxEvicted = (blockSize * iterations) - expectedCached;

    assertEquals(expectedCached, statistics.getCacheBytes());
    assertTrue(statistics.getEvictedBytes()+" of at most "+maxEvicted+" were evicted",
            maxEvicted >= statistics.getEvictedBytes());
  }

  /**
   * Test that a pinned block remains, even after adding 20 * 128 MB = 2.5 GB of blocks
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testPinned() throws IOException, BlockLoadingException, BlockNotFoundException {
    final int blockSize = 128 * 1024 * 1024;
    // Different from other buffers, to tell them apart
    final BlockId pinnedId = randomBlockId(blockSize);

    final BlockLoader pinnedLoader = new MockBlockLoader(pinnedId, new TwosBufferLoader(blockSize), true);

    cache.load(pinnedLoader);
    assertBlockLoaded(pinnedLoader, pinnedId);

    final long iterations = 50;
    for (int i = 0; i < iterations; i++) {
      final BlockId blockId = randomBlockId(blockSize);

      final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(blockSize), false);

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
      final BlockId blockId = randomBlockId(128 * 1024 * 1024);
      BlockLoader loader =
              new MockBlockLoader(blockId, new OnesBufferLoader(128 * 1024 * 1024), true);

      cache.load(loader);
      System.out.println("Loaded " + (128 * (i+1)) + "M");
      loader = null;

      System.out.println("Max Memory: "+Runtime.getRuntime().maxMemory());
      System.out.println("Free Memory: "+Runtime.getRuntime().freeMemory());
      System.out.println("Total Memory: "+Runtime.getRuntime().totalMemory());
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
          final BlockId blockId = randomBlockId(blockSize);
          final boolean pin = (iteration % 20 == 3); // 3, 23, 43
          final AtomicInteger loadCount = new AtomicInteger(0);
          final BlockLoader loader =
                  new MockBlockLoader(blockId, new OnesBufferLoader(blockSize), pin, loadCount, bufferSize);
          if (pin) {
            loadCounts[iteration/20] = loadCount;
          }
          try {
            cache.load(loader);
          } catch (IOException e) {
            throw new RuntimeException(e);
          }
          System.out.println("Loaded " + (blockSize / 1024 / 1024 * (iteration + 1)) + "M");
          System.out.println("Statistics: "+cache.getStatistics());
        }
      });
    }

    for (int i = 0; i < iterations; i++) {
      futures[i].get();
    }

    System.out.println("Statistics: "+cache.getStatistics());
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

    public MockStage(final Cache<BlockId, BlockLoader> internalCache,
                     final MemoryManager memoryManager) {
      this.blockLoaderExecutor = new BlockLoaderExecutor(internalCache, memoryManager);
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
    private final BufferLoader bufferLoader;
    private final boolean pinned;
    private final AtomicInteger loadCount;
    private final int bufferSize;
    private List<byte[]> data;

    public MockBlockLoader(final BlockId blockId, final BufferLoader bufferLoader, final boolean pin) {
      this(blockId, bufferLoader, pin, new AtomicInteger(0), 8 * 1024 * 1024);
    }

    public MockBlockLoader(final BlockId blockId,
                           final BufferLoader bufferLoader,
                           final boolean pin,
                           final AtomicInteger loadCount,
                           final int bufferSize) {
      this.blockId = blockId;
      this.bufferLoader = bufferLoader;
      this.pinned = pin;
      this.loadCount = loadCount;
      this.bufferSize = bufferSize;
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

}
