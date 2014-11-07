package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SurfFSOutputStreamTest {
  private static final Path PATH = new Path("testPath");
  private static final int BLOCKSIZE = 800;
  private static String CACHEADDR = "testCacheAddress";

  private SurfMetaService.Client metaClient;
  private CacheClientManager cacheClientManager;
  private SurfCacheService.Client cacheClient;

  @Before
  public void setUp() throws TException {
    metaClient = mock(SurfMetaService.Client.class);
    when(metaClient.allocateBlock).thenReturn();

    cacheClientManager = mock(CacheClientManager.class);
    cacheClient = mock(SurfCacheService.Client.class);
    when(cacheClientManager.get(CACHEADDR)).thenReturn(cacheClient);
    when(cacheClient.initBlock()).thenReturn(null);
    when(cacheClient.writeData()).thenReturn(null);
    when(cacheClient.finalizeBlock()).thenReturn(null);
    when(cacheClient.completeFile()).thenReturn(null);
  }

  @Test
  public void testBasicWrites() {
    testWrite((byte)1);
    testWrite(150);
    testWrite(512);
    testWrite(BLOCKSIZE);
    testWrite(BLOCKSIZE+4);
    testWrite(30*BLOCKSIZE+3);
  }

  public void testWrite(int dataSize) {
    final SurfFSOutputStream surfFSOutputStream = getSurfFSOutputStream();
    final byte[] data = new byte[dataSize];

    surfFSOutputStream.write(data);
    assertEquals(surfFSOutputStream.getLocalBufWriteCount(), dataSize % surfFSOutputStream.getPacketSize());

    surfFSOutputStream.flush();
    assertEquals(surfFSOutputStream.getLocalBufWriteCount(), 0);
    assertEquals(surfFSOutputStream.getCurBlockOffset(), dataSize % BLOCKSIZE);

    surfFSOutputStream.close();
  }

  public SurfFSOutputStream getSurfFSOutputStream() {
    return new SurfFSOutputStream(PATH, metaClient, cacheClientManager, BLOCKSIZE);
  }
}