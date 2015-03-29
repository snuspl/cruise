package org.apache.reef.inmemory.task.service;

import org.apache.reef.inmemory.common.BlockId;
import org.apache.reef.inmemory.common.BlockMetaFactory;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.common.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.common.exceptions.BlockWritingException;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.common.instrumentation.NullEventRecorder;
import org.apache.reef.inmemory.task.InMemoryCache;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.*;
import static org.mockito.Mockito.when;

public class SurfCacheServerTest {
  private Random random;
  private EventRecorder RECORD;

  @Before
  public void SetUp() {
    random = new Random();
    RECORD = new NullEventRecorder();
  }

  /**
   * Test that binding ephemeral port connecting Surf client to the cache server
   */
  @Test
  public void testFindEphemeralPort() throws IOException {
    final int bufferSize = 8 * 1024 * 1024;
    final SurfCacheServer cacheServer = new SurfCacheServer(null, 0, 0, 1, bufferSize, RECORD);
    final int bindPort = cacheServer.initBindPort();
    assertEquals(bindPort, cacheServer.getBindPort());
    assertNotEquals(0, cacheServer.getBindPort());

    final SurfCacheServer secondServer = new SurfCacheServer(null, 0, 0, 1, bufferSize, RECORD);

    // Should not immediately give back the same port
    final int secondPort = secondServer.initBindPort();
    assertEquals(secondPort, secondServer.getBindPort());
    assertNotEquals(0, secondServer.getBindPort());
    assertNotEquals(cacheServer.getBindPort(), secondServer.getBindPort());

    // Reuse port; should not throw Exception
    final ServerSocket socket = new ServerSocket(secondServer.getBindPort());
    socket.close();
  }

  /**
   * Test that loading data from cache server works well with different block size and buffer size.
   */
  @Test
  public void testBufferSize() throws IOException, BlockLoadingException, BlockNotFoundException, BlockWritingException {
    // Randomly generate buffer with length as blockSize
    final int blockSize = random.nextInt(1024) + 4;
    byte[] buffer = new byte[blockSize];
    random.nextBytes(buffer);

    // Mock objects used to create SurfCacheServer
    BlockMeta blockMeta = Mockito.mock(BlockMeta.class);
    final BlockId id = new BlockId(blockMeta);

    /*
     * Cache is supposed to return the data specified by index.
     * The data is split into chunks which has size of bufferSize
     */
    InMemoryCache cache = Mockito.mock(InMemoryCache.class);
    // choose a number for bufferSize smaller enough than blockSize
    final int bufferSize = random.nextInt(blockSize / 4) + 1;
    when(cache.getLoadingBufferSize()).thenReturn(bufferSize);
    for(int i = 0; i * bufferSize < blockSize; i++) {
      int chunkStart = i * bufferSize;
      int chunkEnd = Math.min((i + 1) * bufferSize, blockSize);
      when(cache.get(id, i)).thenReturn(Arrays.copyOfRange(buffer, chunkStart, chunkEnd));
    }

    final SurfCacheServer cacheServer = new SurfCacheServer(cache, 0, 0, 1, bufferSize, RECORD);

    int nRead = 0;
    byte[] readBuffer = new byte[blockSize];
    while (nRead < blockSize) {
      ByteBuffer loadedBuffer = cacheServer.getData(blockMeta, nRead, blockSize-nRead);
      int nReceived = loadedBuffer.remaining();
      loadedBuffer.get(readBuffer, nRead, loadedBuffer.remaining());
      nRead += nReceived;
    }
    assertArrayEquals(buffer, readBuffer);
  }
}
