package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.AllocatedBlockInfo;
import org.apache.reef.inmemory.common.entity.NodeInfo;
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
import static org.mockito.Matchers.*;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SurfFSOutputStreamTest {
  private static final String PATH = "testPath";
  private static final int BLOCK_SIZE = 800;
  private static String CACHE_ADDR = "testCacheAddress";

  private SurfMetaService.Client metaClient;
  private CacheClientManager cacheClientManager;

  @Before
  public void setUp() throws TException {
    metaClient = mock(SurfMetaService.Client.class);
    final AllocatedBlockInfo allocatedBlockInfo = getAllocatedBlockInfo();
    when(metaClient.allocateBlock(anyString(), anyInt(), anyLong(), anyString())).thenReturn(allocatedBlockInfo);
    doNothing().when(metaClient).completeFile(anyString(), anyLong(), anyLong(), any(NodeInfo.class));

    final SurfCacheService.Client cacheClient = mock(SurfCacheService.Client.class);
    doNothing().when(cacheClient).initBlock(anyString(), anyLong(), anyLong(), any(AllocatedBlockInfo.class));
    doNothing().when(cacheClient).writeData(anyString(), anyLong(), anyLong(), anyLong(), any(ByteBuffer.class), anyBoolean());

    cacheClientManager = mock(CacheClientManager.class);
    when(cacheClientManager.get(CACHE_ADDR)).thenReturn(cacheClient);
  }

  @Test
  public void testBasicWrites() throws IOException {
    testWrite((byte)1); // one byte
    testWrite(150); // < packetSize
    testWrite(512); // == packetSize
    testWrite(BLOCK_SIZE); // one block
    testWrite(BLOCK_SIZE + 4); // > blockSize (one more block)
    testWrite(30 * BLOCK_SIZE + 3); // > many blocks
  }

  public void testWrite(int dataSize) throws IOException {
    if (dataSize <= 0) {
      throw new IllegalArgumentException("dataSize must be bigger than 0");
    }

    final SurfFSOutputStream surfFSOutputStream = getSurfFSOutputStream();
    final byte[] data = new byte[dataSize];

    surfFSOutputStream.write(data);
    assertEquals(dataSize % surfFSOutputStream.getPacketSize(), surfFSOutputStream.getLocalBufWriteCount());

    surfFSOutputStream.flush();
    assertEquals(0, surfFSOutputStream.getLocalBufWriteCount());
    final long expectedCurBlockOffset = dataSize % BLOCK_SIZE == 0 ? (dataSize/BLOCK_SIZE)-1 : dataSize/BLOCK_SIZE;
    assertEquals(expectedCurBlockOffset, surfFSOutputStream.getCurBlockOffset());
    assertEquals(dataSize % BLOCK_SIZE, surfFSOutputStream.getCurBlockInnerOffset());

    surfFSOutputStream.close();
  }

  public SurfFSOutputStream getSurfFSOutputStream() throws UnknownHostException {
    return new SurfFSOutputStream(new Path(PATH), metaClient, cacheClientManager, BLOCK_SIZE);
  }

  public AllocatedBlockInfo getAllocatedBlockInfo() {
    List<NodeInfo> allocatedNodeList = new ArrayList<>();
    allocatedNodeList.add(new NodeInfo(CACHE_ADDR, ""));
    AllocatedBlockInfo allocatedBlock = mock(AllocatedBlockInfo.class);
    when(allocatedBlock.getLocations()).thenReturn(allocatedNodeList);
    return allocatedBlock;
  }
}