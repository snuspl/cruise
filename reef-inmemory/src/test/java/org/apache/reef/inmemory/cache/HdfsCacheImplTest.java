package org.apache.reef.inmemory.cache;

import org.junit.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

public class HdfsCacheImplTest {

  private InMemoryCache cache;
  private Random random = new Random();

  @Before
  public void setUp() {
    cache = new HdfsCacheImpl();
  }

  private BlockId randomBlockId() {
    return new HdfsBlockId(random.nextInt());
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

  @Test
  public void testPutAndGet() {
    BlockId blockId = randomBlockId();
    ByteBuffer buffer = randomOnesBuffer();
    assertNull(cache.get(blockId));
    cache.put(blockId, buffer);
    ByteBuffer getBuffer = cache.get(blockId);
    assertEquals(buffer, getBuffer);
  }

  @Test
  public void testClear() {
    BlockId blockId = randomBlockId();
    ByteBuffer buffer = randomOnesBuffer();
    cache.put(blockId, buffer);
    assertNotNull(cache.get(blockId));
    cache.clear();
    assertNull(cache.get(blockId));
  }
}
