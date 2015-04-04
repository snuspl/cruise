package org.apache.reef.inmemory.metatree;

import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.driver.BaseFsClient;
import org.apache.reef.inmemory.driver.metatree.MetaTree;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.*;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyShort;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.*;

/**
 * TODO: Tests for many other methods are missing & Fix corner cases such as file right under ROOT(e.g. "/hi.txt")
 */
public class MetaTreeTest {
  private EventRecorder eventRecorder;
  private static final long blockSize = 128L * 1024 * 1024;
  private static final short baseReplication = (short)1;

  @Before
  public void setUp() throws Exception {
    eventRecorder = mock(EventRecorder.class);
  }

  /**
   * Test baseFSClient#getFileStatus is called only when the path given does not exist.
   */
  @Test
  public void testGetOrLoadFileMeta() throws IOException {
    final String path = "/this/file";
    final BaseFsClient baseFsClient = mock(BaseFsClient.class);
    when(baseFsClient.getFileStatus(path)).thenReturn(new FileMeta());
    final MetaTree metaTree = new MetaTree(baseFsClient, eventRecorder);
    metaTree.getOrLoadFileMeta(path);
    verify(baseFsClient, times(1)).getFileStatus(path);
    metaTree.getOrLoadFileMeta(path);
    verify(baseFsClient, times(1)).getFileStatus(path);
  }

  /**
   * Test a newly-create file's fileMeta
   */
  @Test
  public void testCreateFileInBaseAndTree() throws Exception {
    final String path = "/this/file";
    final BaseFsClient baseFsClient = mock(BaseFsClient.class);
    doNothing().when(baseFsClient).create(anyString(), anyShort(), anyLong());
    final MetaTree metaTree = new MetaTree(baseFsClient, eventRecorder);

    // Create a file
    metaTree.createFileInBaseAndTree(path, blockSize, baseReplication);
    verify(baseFsClient, times(1)).create(anyString(), anyShort(), anyLong());

    // Should not load meta as the fileMeta is already created(cached)
    final FileMeta outputFileMeta = metaTree.getOrLoadFileMeta(path);
    verify(baseFsClient, times(0)).getFileStatus(path);

    // Check validity of fileMeta
    assertNotNull(outputFileMeta);
    assertEquals(1, outputFileMeta.getFileId());
    assertEquals(blockSize, outputFileMeta.getBlockSize());
    assertEquals(0, outputFileMeta.getFileSize());
    assertEquals(0, outputFileMeta.getBlocksSize());
  }


  /**
   * Test concurrent creation of files under the same directory
   */
  @Test
  public void testConcurrentCreateFileInBaseAndTree() throws Throwable {
    final String directoryPath = "/this/dir";
    final BaseFsClient baseFsClient = mock(BaseFsClient.class);
    doNothing().when(baseFsClient).create(anyString(), anyShort(), anyLong());
    final MetaTree metaTree = new MetaTree(baseFsClient, eventRecorder);
    final int numFiles = 100;
    final ExecutorService executorService = Executors.newCachedThreadPool();

    // Concurrently create files under the same directory
    for (int i = 0; i < numFiles; i++) {
      final int index = i;
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            final String filePath = directoryPath + "/" + String.valueOf(index);
            metaTree.createFileInBaseAndTree(filePath, blockSize, baseReplication);
          } catch (final IOException e) {
            throw new RuntimeException(e);
          }
        }
      });
    }
    executorService.shutdown();
    assertTrue("should complete within 10sec", executorService.awaitTermination(10, TimeUnit.SECONDS));

    long sumOfFileId = 0;
    for (int index = 0; index < numFiles; index++) {
      final String filePath = directoryPath + "/" + String.valueOf(index);

      // Should not load meta as the fileMeta is already created(cached)
      final FileMeta outputFileMeta = metaTree.getOrLoadFileMeta(filePath);
      verify(baseFsClient, times(0)).getFileStatus(filePath);

      // Check validity of fileMeta
      assertNotNull(outputFileMeta);
      assertEquals(blockSize, outputFileMeta.getBlockSize());
      assertEquals(0, outputFileMeta.getFileSize());
      assertEquals(0, outputFileMeta.getBlocksSize());
      sumOfFileId += outputFileMeta.getFileId();
    }
    assertEquals(5050, sumOfFileId);
  }

  /**
   * Test mkdirs
   */
  @Test
  public void testMkdirs() throws Throwable {
    final String directoryPath = "/this/dir";
    final BaseFsClient baseFsClient = mock(BaseFsClient.class);
    when(baseFsClient.mkdirs(directoryPath)).thenReturn(true);
    final MetaTree metaTree = new MetaTree(baseFsClient, eventRecorder);

    assertTrue("mkdirs should succeed", metaTree.mkdirs(directoryPath));
    verify(baseFsClient, times(1)).mkdirs(directoryPath);

    assertTrue("the directory entry should exist", metaTree.exists(directoryPath));
  }

  /**
   * Test concurrent creation of directories under the same directory
   */
  @Test
  public void testConcurrentMkdirs() throws Throwable{
    final String parentDirectoryPath = "/this/dir";
    final BaseFsClient baseFsClient = mock(BaseFsClient.class);
    when(baseFsClient.mkdirs(anyString())).thenReturn(true);
    final MetaTree metaTree = new MetaTree(baseFsClient, eventRecorder);
    final int numDirs = 100;
    final ExecutorService executorService = Executors.newCachedThreadPool();

    // Concurrently create directories under the same directory
    for (int i = 0; i < numDirs; i++) {
      final int index = i;
      executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            final String directoryPath = parentDirectoryPath + "/" + String.valueOf(index);
            assertTrue("mkdirs should succeed", metaTree.mkdirs(directoryPath));
            verify(baseFsClient, times(1)).mkdirs(directoryPath);
          } catch (final Throwable t) {
            throw new RuntimeException(t);
          }
        }
      });
    }
    executorService.shutdown();
    assertTrue("should complete within 10sec", executorService.awaitTermination(10, TimeUnit.SECONDS));

    for (int i = 0; i < numDirs; i++) {
      final int index = i;
      final String directoryPath = parentDirectoryPath + "/" + String.valueOf(index);
      assertTrue("the directory entry should exist for index " + i, metaTree.exists(directoryPath));
    }
  }

  /**
   * Verify that unCacheAll properly clears the cache, and returns the number of
   * previously loaded paths.
   * @throws Throwable
   */
  @Test
  public void testUnCacheAll() throws Throwable {
    final String path = "/this/file";
    final BaseFsClient baseFsClient = mock(BaseFsClient.class);
    when(baseFsClient.getFileStatus(path)).thenReturn(new FileMeta());
    final MetaTree metaTree = new MetaTree(baseFsClient, eventRecorder);

    assertEquals(0, metaTree.unCacheAll());
    metaTree.getOrLoadFileMeta(path);
    assertEquals(1, metaTree.unCacheAll());
    assertEquals(0, metaTree.unCacheAll());
  }
}
