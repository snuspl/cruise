package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.instrumentation.NullEventRecorder;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Verify that SurfFS calls the fallback FS in case of Exceptions while connecting to Surf Driver
 * These tests use mocks to verify that the methods are wired as expected. Actual connecting to
 * a live fallback FS is tested in the SurfFSOpenITCase integration test.
 */
public final class SurfFSFallbackTest {
  private FileSystem surfFs;
  private FileSystem baseFs;
  private final Path path = new Path("/test/path");

  /**
   * Initialize the metaClient that throws an Exception on get
   */
  @Before
  public void setUp() throws Exception {
    final SurfMetaService.Client metaClient = mock(SurfMetaService.Client.class);
    doThrow(TException.class).when(metaClient).load(anyString(), anyString());
    final MetaClientManager metaClientManager = mock(MetaClientManager.class);
    when(metaClientManager.get(anyString())).thenReturn(metaClient);

    baseFs = mock(FileSystem.class);

    surfFs = new SurfFS(baseFs, metaClientManager, new NullEventRecorder());
  }

  /**
   * Test that a call to open calls baseFs on Exception
   */
  @Test
  public void testOpenFallback() throws IOException {
    final int bufferSize = 1024;

    final FSDataInputStream in = surfFs.open(path, bufferSize);
    verify(baseFs).open(path, bufferSize);
  }

  /**
   * Test that a call to getFileBlockLocations calls baseFs on Exception
   */
  @Test
  public void testGetFileBlockLocationsFallback() throws IOException {
    final FileStatus fileStatus = mock(FileStatus.class);
    doReturn(path).when(fileStatus).getPath();
    final long start = 1;
    final long length = 20;

    surfFs.getFileBlockLocations(fileStatus, start, length);
    verify(baseFs).getFileBlockLocations(fileStatus, start, length);
  }
}
