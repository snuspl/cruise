package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.entity.WriteableBlockMeta;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Test SurfFSOutputStream's internal states(offsets)
 */
public final class SurfFSOutputStreamTest {
  private static final String PATH = "testPath";
  private static final int BLOCK_SIZE = 800;
  private static String CACHE_ADDR = "testCacheAddress";

  private SurfMetaService.Client metaClient;
  private CacheClientManager cacheClientManager;

  @Before
  public void setUp() throws TException {
    metaClient = mock(SurfMetaService.Client.class);
    final WriteableBlockMeta WriteableBlockMeta = getWriteableBlockMeta();
    when(metaClient.allocateBlock(anyString(), anyInt(), anyString())).thenReturn(WriteableBlockMeta);
    when(metaClient.completeFile(anyString(), anyLong())).thenReturn(true);

    final SurfCacheService.Client cacheClient = mock(SurfCacheService.Client.class);
    doNothing().when(cacheClient).initBlock(anyLong(), any(WriteableBlockMeta.class));
    doNothing().when(cacheClient).writeData(anyLong(), anyLong(), anyLong(), any(ByteBuffer.class), anyBoolean());

    cacheClientManager = mock(CacheClientManager.class);
    when(cacheClientManager.get(CACHE_ADDR)).thenReturn(cacheClient);
  }

  @Test
  public void testBasicWrites() throws IOException {
    testWrite(1); // one byte
    testWrite(150); // < packetSize
    testWrite(512); // == packetSize
    testWrite(BLOCK_SIZE); // one block
    testWrite(BLOCK_SIZE + 4); // > blockSize (one more block)
    testWrite(30 * BLOCK_SIZE + 3); // > many blocks
    testWrite(1000 * BLOCK_SIZE + 10); // > lots of blocks
  }

  public void testWrite(int fileSize) throws IOException {
    if (fileSize <= 0) {
      throw new IllegalArgumentException("fileSize must be bigger than 0");
    }

    final SurfFSOutputStream surfFSOutputStream = getSurfFSOutputStream();
    final byte[] data = new byte[fileSize];

    surfFSOutputStream.write(data);
    assertEquals(fileSize % SurfFSOutputStream.getPacketSize(), surfFSOutputStream.getLocalBufWriteCount());

    surfFSOutputStream.flush();
    assertEquals(0, surfFSOutputStream.getLocalBufWriteCount());
    assertEquals(fileSize / BLOCK_SIZE, surfFSOutputStream.getCurBlockOffset());
    assertEquals(fileSize % BLOCK_SIZE, surfFSOutputStream.getCurBlockInnerOffset());
    assertEquals(fileSize, BLOCK_SIZE * surfFSOutputStream.getCurBlockOffset() + surfFSOutputStream.getCurBlockInnerOffset());

    surfFSOutputStream.close();
  }

  public SurfFSOutputStream getSurfFSOutputStream() throws UnknownHostException {
    return new SurfFSOutputStream(PATH, metaClient, cacheClientManager, BLOCK_SIZE);
  }

  public WriteableBlockMeta getWriteableBlockMeta() {
    final List<NodeInfo> allocatedNodeList = new ArrayList<>();
    allocatedNodeList.add(new NodeInfo(CACHE_ADDR, ""));

    final WriteableBlockMeta writableBlockMeta = mock(WriteableBlockMeta.class);
    final BlockMeta blockMeta = mock(BlockMeta.class);

    when(blockMeta.getLocations()).thenReturn(allocatedNodeList);
    when(writableBlockMeta.getBlockMeta()).thenReturn(blockMeta);
    return writableBlockMeta;
  }
}