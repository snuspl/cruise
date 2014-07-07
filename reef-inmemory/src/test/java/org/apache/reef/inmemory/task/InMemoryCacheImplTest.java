package org.apache.reef.inmemory.task;

import org.apache.reef.inmemory.task.hdfs.HdfsBlockId;
import org.apache.reef.inmemory.driver.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.driver.exceptions.BlockNotFoundException;
import org.junit.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

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
   * Put a small buffer in the task, and check correctness of get after put
   */
  @Test
  public void testPutAndGet() throws BlockLoadingException, BlockNotFoundException {
    BlockId blockId = randomBlockId();
    assertBlockNotFound(blockId);

    ByteBuffer buffer = randomOnesBuffer();
    cache.put(blockId, buffer.array());
    byte[] getData = cache.get(blockId);
    assertEquals(buffer.array(), getData);
  }

  /**
   * Put a small buffer in the task, and check correctness of clear
   */
  @Test
  public void testClear() throws BlockLoadingException, BlockNotFoundException {
    BlockId blockId = randomBlockId();
    ByteBuffer buffer = randomOnesBuffer();
    cache.put(blockId, buffer.array());
    assertNotNull(cache.get(blockId));
    cache.clear();
    assertBlockNotFound(blockId);
  }
}
