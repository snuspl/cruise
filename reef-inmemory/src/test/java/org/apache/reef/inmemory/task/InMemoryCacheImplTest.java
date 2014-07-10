package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.junit.*;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.*;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * Tests for HdfsCache
 */
public final class InMemoryCacheImplTest {

  private InMemoryCache cache;
  private Random random = new Random();

  @Before
  public void setUp() {
    cache = new InMemoryCacheImpl();
  }

  private BlockId randomBlockId() {
    return new HdfsBlockId(random.nextLong(),
            random.nextLong(),
            random.nextLong(),
            Long.toString(random.nextLong()),
            Long.toString(random.nextLong()));
  }

  private byte[] ones(int length) {
    byte[] buf = new byte[length];
    Arrays.fill(buf, (byte) 1);
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
    final BlockId blockId = randomBlockId();
    assertBlockNotFound(blockId);
    final byte[] buffer = randomOnesBuffer();

    final BlockLoader loader = mock(BlockLoader.class);
    when(loader.getBlockId()).thenReturn(blockId);
    when(loader.loadBlock()).thenReturn(buffer);

    cache.load(loader);
    byte[] getData = cache.get(blockId);
    assertEquals(buffer, getData);
  }

  /**
   * Put a small buffer in the task, and check correctness of clear
   */
  @Test
  public void testClear() throws BlockLoadingException, BlockNotFoundException, IOException {
    final BlockId blockId = randomBlockId();
    final byte[] buffer = randomOnesBuffer();

    final BlockLoader loader = mock(BlockLoader.class);
    when(loader.getBlockId()).thenReturn(blockId);
    when(loader.loadBlock()).thenReturn(buffer);

    cache.load(loader);
    assertNotNull(cache.get(blockId));
    cache.clear();
    assertBlockNotFound(blockId);
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
  public void testConcurrentLoads() throws InterruptedException, IOException, BlockLoadingException, BlockNotFoundException, ExecutionException {
    final int numThreads = 10; // Must be > 3
    final ExecutorService e = Executors.newFixedThreadPool(numThreads);

    final BlockId blockId = randomBlockId();
    assertBlockNotFound(blockId);
    final byte[] buffer = randomOnesBuffer();

    final BlockLoader[] loaders = new BlockLoader[numThreads];
    final Future<?>[] futures = new Future<?>[numThreads];

    // Start long-running block load
    {
      final BlockLoader loader = mock(BlockLoader.class);
      when(loader.getBlockId()).thenReturn(blockId);
      when(loader.loadBlock()).thenAnswer(new Answer<byte[]>() {
        @Override
        public byte[] answer(InvocationOnMock invocation) throws Throwable {
          Thread.sleep(3000); // Sleep 3 seconds
          return buffer;
        }
      });
      loaders[0] = loader;
      futures[0] = e.submit(new Runnable() {
        @Override
        public void run() {
          try {
            cache.load(loader);
          } catch (IOException e1) {
            fail("IOException " + e1);
          }
        }
      });
    }

    Thread.sleep(500); // Allow first block load to start

    // Try to load same block
    for (int i = 1; i < numThreads/2; i++) {
      final BlockLoader loader = mock(BlockLoader.class);
      when(loader.getBlockId()).thenReturn(blockId); // Load called on same blockId
      when(loader.loadBlock()).thenReturn(randomOnesBuffer());
      loaders[i] = loader;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          // First test if there is a block loading exception as expected
          try {
            cache.get(blockId);
            fail("Expected BlockLoadingException");
          } catch (BlockLoadingException e1) {
            // Expected
          } catch (BlockNotFoundException e1) {
            fail("Expected BlockLoadingException");
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
      final BlockLoader loader = mock(BlockLoader.class);
      final BlockId randomId = randomBlockId();
      final byte[] randomBuffer = randomOnesBuffer();
      when(loader.getBlockId()).thenReturn(randomId); // Load called on different blockId
      when(loader.loadBlock()).thenReturn(randomBuffer);
      loaders[i] = loader;
      futures[i] = e.submit(new Runnable() {
        @Override
        public void run() {
          // First test if there is a block not found exception as expected
          try {
            cache.get(randomId);
            fail("Expected BlockNotFoundException");
          } catch (BlockLoadingException e1) {
            fail("Expected BlockNotFoundException");
          } catch (BlockNotFoundException e1) {
            // Expected
          }

          // Then call load
          try {
            cache.load(loader);
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

    assertEquals("Check first block request loaded", buffer, cache.get(blockId));

    // Verify first loader called once
    verify(loaders[0], times(1)).loadBlock();

    // Verify loaders for same blockId not called
    for (int i = 1; i < numThreads/2; i++) {
      verify(loaders[i], never()).loadBlock();
    }

    // Verify loaders for different blockId called once
    for (int i = numThreads/2; i < numThreads; i++) {
      verify(loaders[i], times(1)).loadBlock();
    }
  }
}
