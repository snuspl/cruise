package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.junit.*;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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

  private ByteBuffer onesBuffer(int length) {
    byte one = (byte) 1;
    byte[] buf = new byte[length];
    Arrays.fill(buf, one);
    return ByteBuffer.wrap(buf);
  }

  private ByteBuffer randomOnesBuffer() {
    return onesBuffer(1 + random.nextInt(10));
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
    final ByteBuffer buffer = randomOnesBuffer();

    final BlockLoader loader = mock(BlockLoader.class);
    when(loader.getBlockId()).thenReturn(blockId);
    when(loader.loadBlock()).thenReturn(buffer.array());

    cache.load(loader);
    byte[] getData = cache.get(blockId);
    assertEquals(buffer.array(), getData);
  }

  /**
   * Put a small buffer in the task, and check correctness of clear
   */
  @Test
  public void testClear() throws BlockLoadingException, BlockNotFoundException, IOException {
    final BlockId blockId = randomBlockId();
    final ByteBuffer buffer = randomOnesBuffer();

    final BlockLoader loader = mock(BlockLoader.class);
    when(loader.getBlockId()).thenReturn(blockId);
    when(loader.loadBlock()).thenReturn(buffer.array());

    cache.load(loader);
    assertNotNull(cache.get(blockId));
    cache.clear();
    assertBlockNotFound(blockId);
  }
}
