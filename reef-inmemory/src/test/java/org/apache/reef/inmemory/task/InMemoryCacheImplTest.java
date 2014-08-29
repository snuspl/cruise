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
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

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
    statistics = new CacheStatistics();
    memoryManager = new MemoryManager(statistics, 384 * 1024 * 1024); // Should run in junit fork mode
    loadingStage = new MockStage(memoryManager);
    cache = new InMemoryCacheImpl(memoryManager, loadingStage, 3);
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

  private void assertBlockNotFound(BlockId blockId) {
    try {
      cache.get(blockId);
      fail("Should have thrown BlockNotFoundException");
    } catch (BlockNotFoundException e) {
      assertTrue(true);
    } catch (Exception e) {
      fail("Should have thrown BlockNotFoundException");
    }
  }

  /**
   * Put a small buffer in the task, and check correctness of get after load
   */
  @Test
  public void testPutAndGet() throws BlockLoadingException, BlockNotFoundException, IOException {
    final BlockId blockId = randomBlockId(1024);
    assertBlockNotFound(blockId);

    final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(1024), false);

    cache.load(loader, false);
    byte[] getData = cache.get(blockId);
    assertArrayEquals(ones(1024), getData);
  }

  /**
   * Put a small buffer in the task, and check correctness of clear
   */
  @Test
  public void testClear() throws BlockLoadingException, BlockNotFoundException, IOException {
    final BlockId blockId = randomBlockId(1024);

    final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(1024), false);

    cache.load(loader, false);
    assertNotNull(cache.get(blockId));
    cache.clear();
    assertBlockNotFound(blockId);

    assertEquals(0, statistics.getCacheBytes());
    assertEquals(0, statistics.getPinnedBytes());
    assertEquals(0, statistics.getEvictedBytes());
  }

  /**
   * Test statistics after load
   */
  @Test
  public void testStatistics() throws Exception {
    final BlockId blockId = randomBlockId(1024);

    final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(1024), false);

    cache.load(loader, false);
    assertEquals(1024, cache.getStatistics().getCacheBytes());
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
    assertBlockNotFound(blockId);

    // Start long-running block load
    final BlockLoader firstLoader;
    {
      firstLoader = new SleepingBlockLoader(blockId, false, firstLoadBuffer, 3000, new AtomicInteger(0));
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
//
//  /**
//   * Tests for correct cache behavior on concurrent load calls to the same blockId.
//   *
//   * Start a cache.load() that takes 5 seconds to complete.
//   * Then start multiple concurrent cache.load() on the same blockId.
//   * Verify that only the first cache.load() succeeded, and none of the
//   * subsequent load() calls resulted in a loader.loadBlock() invocation.
//   */
//  @Test
//  public void testConcurrentLoads() throws Exception {
//    final int numThreads = 10; // Must be > 3
//    final ExecutorService e = Executors.newFixedThreadPool(numThreads);
//
//    final byte[] firstLoadBuffer = twos(1024);
//    final BlockId blockId = randomBlockId(firstLoadBuffer.length);
//    assertBlockNotFound(blockId);
//
//    final BlockLoader[] loaders = new BlockLoader[numThreads];
//    final Future<?>[] futures = new Future<?>[numThreads];
//    final AtomicInteger[] loadCounts = new AtomicInteger[numThreads];
//    for (int i = 0; i < numThreads; i++) {
//      loadCounts[i] = new AtomicInteger(0);
//    }
//
//    // Start long-running block load
//    {
//      final BlockLoader firstLoader = new SleepingBlockLoader(blockId, false, firstLoadBuffer, 3000, loadCounts[0]);
//
//      loaders[0] = firstLoader;
//      futures[0] = e.submit(new Runnable() {
//        @Override
//        public void run() {
//          try {
//            cache.load(firstLoader, false);
//          } catch (IOException e1) {
//            fail("IOException " + e1);
//          }
//        }
//      });
//    }
//
//    Thread.sleep(500); // Allow first block load to start
//
//    // Try to load same block
//    for (int i = 1; i < numThreads/2; i++) {
//      // Load called on same blockId
//      final BlockLoader loader = new MockBlockLoader(blockId, ones(1024), false, loadCounts[i]);
//      loaders[i] = loader;
//      futures[i] = e.submit(new Runnable() {
//        @Override
//        public void run() {
//          // First test if there is a block loading exception as expected
//          try {
//            cache.get(blockId);
//            fail("Expected BlockLoadingException but did not get an exception");
//          } catch (BlockLoadingException e1) {
//            // Expected
//          } catch (BlockNotFoundException e1) {
//            fail("Expected BlockLoadingException but received BlockNotFoundException");
//          }
//
//          // Then call load
//          try {
//            cache.load(loader, false);
//          } catch (IOException e1) {
//            fail("IOException "+e1);
//          }
//        }
//      });
//    }
//
//    // Try to load different block
//    for (int i = numThreads/2; i < numThreads; i++) {
//      final byte[] randomBuffer = randomOnesBuffer();
//      final BlockId randomId = randomBlockId(randomBuffer.length);
//      // Load called on different blockId
//      final BlockLoader loader = new MockBlockLoader(randomId, randomBuffer, false, loadCounts[i]);
//      loaders[i] = loader;
//      futures[i] = e.submit(new Runnable() {
//        @Override
//        public void run() {
//          // First test if there is a block not found exception as expected
//          try {
//            cache.get(randomId);
//            fail("Expected BlockNotFoundException but did not get an exception");
//          } catch (BlockLoadingException e1) {
//            fail("Expected BlockNotFoundException but received BlockLoadingException");
//          } catch (BlockNotFoundException e1) {
//            // Expected
//          }
//
//          // Then call load
//          try {
//            cache.load(loader, false);
//            assertEquals(randomBuffer, cache.get(randomId));
//          } catch (final IOException | BlockLoadingException | BlockNotFoundException e1) {
//            fail("Exception " + e1);
//          }
//        }
//      });
//    }
//
//    e.shutdown();
//    e.awaitTermination(10, TimeUnit.SECONDS);
//
//    for (int i = 0; i < numThreads; i++) {
//      futures[i].get();
//    }
//
//    assertEquals("Check first block request loaded", firstLoadBuffer, cache.get(blockId));
//
//    // Verify first loader called once
//    assertEquals(1, loadCounts[0].get());
//
//    // Verify loaders for same blockId not called
//    for (int i = 1; i < numThreads/2; i++) {
//      assertEquals(0, loadCounts[i].get());
//    }
//
//    // Verify loaders for different blockId called once
//    for (int i = numThreads/2; i < numThreads; i++) {
//      assertEquals(1, loadCounts[i].get());
//    }
//  }
//
  private void assertRemovalsNotFound(final CacheUpdates updates) {
    assertTrue(updates.getRemovals().size() > 0);
    for (final BlockId blockId : updates.getRemovals()) {
      try {
        cache.get(blockId);
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
  public void testEviction() throws Exception {
    final int blockSize = 128 * 1024 * 1024;
    final long iterations = 50;
    for (int i = 0; i < iterations; i++) {
      final BlockId blockId = randomBlockId(blockSize);
      BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(blockSize), false);

      cache.load(loader, false);
      System.out.println("Loaded " + (blockSize / 1024 / 1024 * (i+1)) + "M");
      System.out.println("Statistics: "+cache.getStatistics());
      Thread.sleep(10);
    }

    System.out.println("Statistics: "+cache.getStatistics());
    assertEquals(iterations * blockSize, statistics.getCacheBytes() + statistics.getEvictedBytes());
  }

  /**
   * Test that adding 20 * 128 MB = 2.5 GB of blocks concurrently does not cause an OutOfMemory exception
   */
  @Test
  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
  public void testConcurrentEviction() throws IOException, ExecutionException, InterruptedException {
    final int blockSize = 128 * 1024 * 1024;

    final int numThreads = 10;
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);

    final int iterations = 50;

    final Future<?>[] futures = new Future<?>[iterations];

    for (int i = 0; i < iterations; i++) {
      final int iteration = i;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          final BlockId blockId = randomBlockId(blockSize);
          final BlockLoader loader =
                  new MockBlockLoader(blockId, new OnesBufferLoader(blockSize), false);

          try {
            cache.load(loader, false);
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
    assertEquals((long)iterations * blockSize, statistics.getCacheBytes() + statistics.getEvictedBytes());
  }

//  /**
//   * Test that a pinned block remains, even after adding 20 * 128 MB = 2.5 GB of blocks
//   */
//  @Test
//  @Category(org.apache.reef.inmemory.common.IntensiveTests.class)
//  public void testPinned() throws IOException, BlockLoadingException, BlockNotFoundException {
//    final int blockSize = 128 * 1024 * 1024;
//    // Different from other buffers, to tell them apart
//    final BlockId pinnedId = randomBlockId(blockSize);
//
//    final BlockLoader pinnedLoader = new MockBlockLoader(pinnedId, new TwosBufferLoader(blockSize), true);
//
//    cache.load(pinnedLoader, true);
//    assertEquals((byte) 2, cache.get(pinnedId)[0]);
//
//    final long iterations = 50;
//    for (int i = 0; i < iterations; i++) {
//      final BlockId blockId = randomBlockId(blockSize);
//
//      final BlockLoader loader = new MockBlockLoader(blockId, new OnesBufferLoader(blockSize), false);
//
//      cache.load(loader, false);
//      System.out.println("Loaded " + (blockSize / 1024 / 1024 * (i + 1)) + "M");
//    }
//
//    assertEquals((byte) 2, cache.get(pinnedId)[0]);
//
//    final CacheStatistics statistics = cache.getStatistics();
//    assertEquals(iterations * blockSize, statistics.getCacheBytes() + statistics.getEvictedBytes());
//  }
//
//  @Test
//  public void testTooManyPinned() throws Exception {
//    final long iterations = 50;
//    for (int i = 0; i < iterations; i++) {
//      final BlockId blockId = randomBlockId(128 * 1024 * 1024);
//      BlockLoader loader =
//              new MockBlockLoader(blockId, new OnesBufferLoader(128 * 1024 * 1024), true);
//
//      cache.load(loader, true);
//      System.out.println("Loaded " + (128 * (i+1)) + "M");
//      loader = null;
//
//      System.out.println("Max Memory: "+Runtime.getRuntime().maxMemory());
//      System.out.println("Free Memory: "+Runtime.getRuntime().freeMemory());
//      System.out.println("Total Memory: "+Runtime.getRuntime().totalMemory());
//    }
//  }

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
    private final byte[] data;
    private final int milliseconds;
    private boolean loadDone;

    private final AtomicInteger loadCount;

    public SleepingBlockLoader(final BlockId blockId,
                               final boolean pin,
                               final byte[] data,
                               final int milliseconds,
                               final AtomicInteger loadCount) {
      this.blockId = blockId;
      this.pinned = pin;
      this.data = data;
      this.milliseconds = milliseconds;
      this.loadCount = loadCount;
      this.loadDone = false;
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
    public byte[] getData() throws BlockLoadingException {
      if (loadDone) {
        return data;
      } else {
        throw new BlockLoadingException();
      }
    }
  }

  private interface BufferLoader {
    public byte[] loadBuffer();
  }

  private final class OnesBufferLoader implements BufferLoader {
    private final int length;
    public OnesBufferLoader(final int length) {
      this.length = length;
    }

    @Override
    public byte[] loadBuffer() {
      return ones(length);
    }
  }

  private final class TwosBufferLoader implements BufferLoader {
    private final int length;
    public TwosBufferLoader(final int length) {
      this.length = length;
    }

    @Override
    public byte[] loadBuffer() {
      return twos(length);
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
    private byte[] data;

    public MockBlockLoader(final BlockId blockId, final BufferLoader bufferLoader, final boolean pin) {
      this(blockId, bufferLoader, pin, new AtomicInteger(0));
    }

    public MockBlockLoader(final BlockId blockId,
                           final BufferLoader bufferLoader,
                           final boolean pin,
                           final AtomicInteger loadCount) {
      this.blockId = blockId;
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
    public boolean isPinned() {
      return pinned;
    }

    @Override
    public byte[] getData() throws BlockLoadingException {
      return data;
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

    @Override
    public String toString() {
      return "MockBlockId{" +
              "blockId='" + blockId + '\'' +
              '}';
    }
  }


}
