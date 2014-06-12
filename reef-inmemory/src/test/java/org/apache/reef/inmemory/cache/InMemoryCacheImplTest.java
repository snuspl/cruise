package org.apache.reef.inmemory.cache;

import org.junit.*;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;

public class InMemoryCacheImplTest {

  private InMemoryCache cache;
  private Random random = new Random();

  @Before
  public void setUp() {
    cache = new InMemoryCacheImpl();
  }

  private FileBlock randomBlock() {
    return new FileBlock(Integer.toString(random.nextInt()), random.nextInt(100), random.nextInt(100));
  }

  private ByteBuffer onesBuffer(int length) {
    byte one = (byte) 1;
    byte[] buf = new byte[length];
    Arrays.fill(buf, one);
    return ByteBuffer.wrap(buf);
  }

  @Test
  public void testPutAndGet() {
    FileBlock block = randomBlock();
    ByteBuffer buffer = onesBuffer(block.getLength());
    assertNull(cache.get(block));
    cache.put(block, buffer);
    ByteBuffer getBuffer = cache.get(block);
    assertEquals(buffer, getBuffer);
  }

  @Test
  public void testClear() {
    FileBlock block = randomBlock();
    ByteBuffer buffer = onesBuffer(block.getLength());
    cache.put(block, buffer);
    assertNotNull(cache.get(block));
    cache.clear();
    assertNull(cache.get(block));
  }
}
