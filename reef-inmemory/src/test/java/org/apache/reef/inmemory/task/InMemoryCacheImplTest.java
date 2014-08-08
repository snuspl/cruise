package org.apache.reef.inmemory.task;

import com.microsoft.wake.EStage;
import org.apache.reef.inmemory.common.CacheStatistics;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for InMemoryCacheImpl
 */
public final class InMemoryCacheImplTest {

  private CacheStatistics statistics;
  private EStage<BlockLoader> loadingStage;
  private InMemoryCache cache;
  private Random random = new Random();

  @Before
  public void setUp() {
    statistics = new CacheStatistics();
    loadingStage = new MockStage(statistics);
    cache = new InMemoryCacheImpl(statistics, loadingStage, 3);
  }

  private BlockId randomBlockId(long length) {
    return new MockBlockId(Long.toString(random.nextLong()), length);
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

  private BlockLoader mockBlockLoader(final BlockId blockId,
                                      final byte[] buffer) {
    try {
      final BlockLoader loader = mock(BlockLoader.class);
      when(loader.getBlockId()).thenReturn(blockId);
      when(loader.getData()).thenReturn(buffer);
      return loader;
    } catch (BlockLoadingException e) {
      throw new RuntimeException("Initializing mock loader failed");
    }
  }

  /**
   * Put a small buffer in the task, and check correctness of get after load
   */
  @Test
  public void testPutAndGet() throws BlockLoadingException, BlockNotFoundException, IOException {
    final byte[] buffer = randomOnesBuffer();
    final BlockId blockId = randomBlockId(buffer.length);
    assertBlockNotFound(blockId);

    final BlockLoader loader = mockBlockLoader(blockId, buffer);

    cache.load(loader, false);
    byte[] getData = cache.get(blockId);
    assertEquals(buffer, getData);
  }

  /**
   * Put a small buffer in the task, and check correctness of clear
   */
  @Test
  public void testClear() throws BlockLoadingException, BlockNotFoundException, IOException {
    final byte[] buffer = randomOnesBuffer();
    final BlockId blockId = randomBlockId(buffer.length);

    final BlockLoader loader = mockBlockLoader(blockId, buffer);

    cache.load(loader, false);
    assertNotNull(cache.get(blockId));
    cache.clear();
    assertBlockNotFound(blockId);
  }

  /**
   * Test statistics after load
   * TODO: test statistics during load
   */
  @Test
  public void testStatistics() throws Exception {
    final byte[] buffer = randomOnesBuffer();
    final BlockId blockId = randomBlockId(buffer.length);

    final BlockLoader loader = mockBlockLoader(blockId, buffer);

    cache.load(loader, false);
    assertEquals(buffer.length, cache.getStatistics().getCacheBytes());
    assertEquals(0, cache.getStatistics().getLoadingBytes());

    cache.clear();
    assertEquals(0, cache.getStatistics().getCacheBytes());
    assertEquals(0, cache.getStatistics().getLoadingBytes());
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
    assertBlockNotFound(blockId);

    final BlockLoader[] loaders = new BlockLoader[numThreads];
    final Future<?>[] futures = new Future<?>[numThreads];

    // Start long-running block load
    final AtomicInteger firstNumLoads = new AtomicInteger(0);
    final BlockLoader firstLoader;
    {
      firstLoader = new SleepingBlockLoader(blockId, firstLoadBuffer, 3000, firstNumLoads);

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
      final BlockLoader loader = mockBlockLoader(blockId, ones(1024));
      loaders[i] = loader;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          // First test if there is a block loading exception as expected
          try {
            cache.get(blockId);
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
      final BlockLoader loader = mockBlockLoader(randomId, randomBuffer);
      loaders[i] = loader;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          // First test if there is a block not found exception as expected
          try {
            cache.get(randomId);
            fail("Expected BlockNotFoundException but did not get an exception");
          } catch (BlockLoadingException e1) {
            fail("Expected BlockNotFoundException but received BlockLoadingException");
          } catch (BlockNotFoundException e1) {
            // Expected
          }

          // Then call load
          try {
            cache.load(loader, false);
            assertEquals(randomBuffer, cache.get(randomId));
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

    assertEquals("Check first block request loaded", firstLoadBuffer, cache.get(blockId));

    // Verify first loader called once (first loader does not use mockito)
    assertEquals(1, firstNumLoads.get());

    // Verify loaders for same blockId not called
    for (int i = 1; i < numThreads/2; i++) {
      verify(loaders[i], never()).loadBlock();
    }

    // Verify loaders for different blockId called once
    for (int i = numThreads/2; i < numThreads; i++) {
      verify(loaders[i], times(1)).loadBlock();
    }
  }


  /**
   * Test that pinning too many blocks gives an OutOfMemoryError.
   * If this test does not pass, then the unit test is running with too much memory!
   * TODO: base the amount of memory added on the Runtime max memory * multiplier
   */
  @Test(expected=OutOfMemoryError.class)
  public void testTooManyPinned() throws Exception {
    for (int i = 0; i < 20; i++) {
      final byte[] buffer = ones(128 * 1024 * 1024);
      final BlockId blockId = randomBlockId(buffer.length);
      final BlockLoader loader = new MockBlockLoader(blockId, buffer);

      cache.load(loader, true);
      System.out.println("Loaded " + (128 * (i+1)) + "M");
    }
  }

  /**
   * Test that adding 20 * 128 MB = 2.5 GB of blocks does not cause an OutOfMemory exception
   */
  @Test
  public void testEviction() throws IOException {
    for (int i = 0; i < 20; i++) {
      final byte[] buffer = ones(128 * 1024 * 1024);
      final BlockId blockId = randomBlockId(buffer.length);
      final BlockLoader loader = new MockBlockLoader(blockId, buffer);

      cache.load(loader, false);
      System.out.println("Loaded " + (128 * (i+1)) + "M");
    }
  }

  /**
   * Test that a pinned block remains, even after adding 20 * 128 MB = 2.5 GB of blocks
   */
  @Test
  public void testPinned() throws IOException, BlockLoadingException, BlockNotFoundException {
    // Different from other buffers, to tell them apart
    final byte[] pinnedBuffer = twos(128 * 1024 * 1024);
    final BlockId pinnedId = randomBlockId(pinnedBuffer.length);

    final BlockLoader pinnedLoader = new MockBlockLoader(pinnedId, pinnedBuffer);

    cache.load(pinnedLoader, true);
    assertEquals(pinnedBuffer, cache.get(pinnedId));

    for (int i = 0; i < 20; i++) {
      final byte[] buffer = ones(128 * 1024 * 1024);
      final BlockId blockId = randomBlockId(buffer.length);

      final BlockLoader loader = new MockBlockLoader(blockId, buffer);

      cache.load(loader, false);
      System.out.println("Loaded " + (128 * (i + 1)) + "M");
    }

    assertEquals(pinnedBuffer, cache.get(pinnedId));
  }

  /**
   * Immediately load block, instead of using a stage
   */
  private static class MockStage implements EStage<BlockLoader> {

    private final BlockLoaderExecutor blockLoaderExecutor;

    public MockStage(final CacheStatistics statistics) {
      this.blockLoaderExecutor = new BlockLoaderExecutor(statistics);
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
    private final byte[] data;
    private final int milliseconds;
    private boolean loadDone;

    private final AtomicInteger numLoadCalled;

    public SleepingBlockLoader(final BlockId blockId,
                               final byte[] data,
                               final int milliseconds,
                               final AtomicInteger numLoadCalled) {
      this.blockId = blockId;
      this.data = data;
      this.milliseconds = milliseconds;
      this.numLoadCalled = numLoadCalled;
      this.loadDone = false;
    }

    @Override
    public void loadBlock() throws IOException {
      numLoadCalled.incrementAndGet();
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
    public byte[] getData() throws BlockLoadingException {
      if (loadDone) {
        return data;
      } else {
        throw new BlockLoadingException();
      }
    }
  }

  /**
   * Not using Mockito because Mockito causes OutOfMemory exception on memory-intensive tests
   */
  private static class MockBlockLoader implements BlockLoader {

    private final BlockId blockId;
    private final byte[] data;

    public MockBlockLoader(final BlockId blockId, final byte[] data) {
      this.blockId = blockId;
      this.data = data;
    }

    @Override
    public void loadBlock() throws IOException {
      // Do nothing, data is already "loaded"
    }

    @Override
    public BlockId getBlockId() {
      return blockId;
    }

    @Override
    public byte[] getData() throws BlockLoadingException {
      return data;
    }
  }

  private static final class MockBlockId implements BlockId {

    private final String blockId;
    private final long blockSize;

    public MockBlockId(final String blockId,
                       final long blockSize) {
      this.blockId = blockId;
      this.blockSize = blockSize;
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

      if (blockSize != that.blockSize) return false;
      if (!blockId.equals(that.blockId)) return false;

      return true;
    }

    @Override
    public int hashCode() {
      int result = blockId.hashCode();
      result = 31 * result + (int) (blockSize ^ (blockSize >>> 32));
      return result;
    }
  }


}
