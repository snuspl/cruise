package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.service.SurfCacheService;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SurfFSOutputStreamTest {
  private static final Path PATH = new Path("testPath");
  private static final int BLOCKSIZE = 512;
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
    when(cacheClient.initBlock()).thenReturn();
    when(cacheClient.writeData()).thenReturn();
    when(cacheClient.finalizeBlock()).thenReturn();
    when(cacheClient.completeFile()).thenReturn();
  }

  @Test
  public void writeSingleByte() {
    final SurfFSOutputStream surfFSOutputStream = getSurfFSOutputStream();
    surfFSOutputStream.write(0);
  }

  @Test
  public void writeSingleExactBlock() {
    final SurfFSOutputStream surfFSOutputStream = getSurfFSOutputStream();
    final byte[] data = new byte[BLOCKSIZE];
    surfFSOutputStream.write(data);
    surfFSOutputStream.close();
  }

  @Test
  public void writeSingleBlockAndOneMore() {
    final SurfFSOutputStream surfFSOutputStream = getSurfFSOutputStream();
    final byte[] data = new byte[BLOCKSIZE+1];
    surfFSOutputStream.write(data);
    surfFSOutputStream.close();
  }

  @Test
  public void writeMultipleBlocks() {
    final SurfFSOutputStream surfFSOutputStream = getSurfFSOutputStream();
  }

  @Test
  public void flushAndWriteAgain() {
    final SurfFSOutputStream surfFSOutputStream = getSurfFSOutputStream();
  }

  public SurfFSOutputStream getSurfFSOutputStream() {
    return new SurfFSOutputStream(PATH, metaClient, cacheClientManager, BLOCKSIZE);
  }
}