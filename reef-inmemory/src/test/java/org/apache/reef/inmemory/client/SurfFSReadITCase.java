package org.apache.reef.inmemory.client;

import org.apache.reef.inmemory.common.SurfLauncher;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.hadoop.io.IOUtils;
import org.apache.reef.inmemory.common.ITUtils;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Arrays;
import java.util.logging.Logger;

import static org.junit.Assert.*;

/**
 * Test reading HDFS files(initially uncached) via Surf
 */
public final class SurfFSReadITCase {

  private static final Logger LOG = Logger.getLogger(SurfFSReadITCase.class.getName());

  private static FileSystem baseFs;
  private static SurfFS surfFs;

  private static final byte[] CHUNK = new byte[]{(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};

  private static final String TESTDIR = ITUtils.getTestDir();

  private static final String SHORT_FILE_PATH = TESTDIR+"/"+"COUNT.short";
  private static final int SHORT_FILE_NUM_CHUNKS = 1;

  private static final String LONG_FILE_PATH = TESTDIR+"/"+"COUNT.long";
  private static final int LONG_FILE_NUM_CHUNKS = 140;

  private static final String SURF = "surf";
  private static final String SURF_ADDRESS = "localhost:18000";

  private static final int DFS_BLOCK_SIZE_VALUE = 512;

  private static final SurfLauncher surfLauncher = new SurfLauncher();

  /**
   * Connect to HDFS cluster for integration test, and create test elements.
   * Don't run destructive tests on the elements created here.
   * Launch REEF instance.
   */
  @BeforeClass
  public static void setUpClass() throws IOException, InjectionException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Reduce blocksize to 512 bytes, to test multiple blocks
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_VALUE);

    baseFs = ITUtils.getHdfs(hdfsConfig);
    baseFs.mkdirs(new Path(TESTDIR));

    final FSDataOutputStream stream1 = baseFs.create(new Path(SHORT_FILE_PATH));
    for (int i = 0; i < SHORT_FILE_NUM_CHUNKS; i++) {
      stream1.write(CHUNK);
    }
    stream1.close();

    final FSDataOutputStream stream2 = baseFs.create(new Path(LONG_FILE_PATH));
    for (int i = 0; i < LONG_FILE_NUM_CHUNKS; i++) {
      stream2.write(CHUNK);
    }
    stream2.close();

    surfLauncher.launch(baseFs);

    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());
    conf.setInt(SurfFS.CACHECLIENT_BUFFER_SIZE_KEY, 64);
    // Increase retries on no progress, because loading from MiniCluster is slower when running as single-machine multi-threaded tests
    conf.setInt(LoadProgressManagerImpl.LOAD_MAX_NO_PROGRESS_KEY, 10);

    surfFs = new SurfFS();
    surfFs.initialize(URI.create(SURF+"://"+SURF_ADDRESS), conf);
  }

  /**
   * Remove all directories.
   * Wait for Surf to shutdown on timeout.
   */
  @AfterClass
  public static void tearDownClass() throws Exception {
    baseFs.delete(new Path(TESTDIR), true);
    surfLauncher.close();
  }

  private void assertBufEqualsChunk(final byte[] buf, int position, int length) {
    for (int i = 0; i < length; i++) {
      assertEquals("At index "+i, CHUNK[(i + position) % CHUNK.length], buf[i]);
    }
  }

  private void read(final FileSystem fs, final String path, final int numChunks) throws IOException {
    FSDataInputStream in = fs.open(new Path(path));

    byte[] readBuf = new byte[numChunks * CHUNK.length];

    int bytesRead = in.read(0, readBuf, 0, readBuf.length);
    assertEquals(bytesRead, readBuf.length);
    assertBufEqualsChunk(readBuf, 0, readBuf.length);
  }

  private void copyBytes(final FileSystem fs, final String path, final int size) throws IOException {
    FSDataInputStream in = fs.open(new Path(path));
    OutputStream out = new ByteArrayOutputStream(size * CHUNK.length);

    IOUtils.copyBytes(in, out, size * CHUNK.length);
  }

  @Test
  public void testRead() throws IOException {
    read(surfFs, SHORT_FILE_PATH, SHORT_FILE_NUM_CHUNKS);
    read(surfFs, LONG_FILE_PATH, LONG_FILE_NUM_CHUNKS);
    // TODO: Check various boundary conditions
  }

  @Test
  public void testCopyBytes() throws IOException {
    copyBytes(surfFs, SHORT_FILE_PATH, SHORT_FILE_NUM_CHUNKS);
    copyBytes(surfFs, LONG_FILE_PATH, LONG_FILE_NUM_CHUNKS);
    // TODO: Check various boundary conditions
  }

  private void assertSeekThenReadEqualsChunk(final FSDataInputStream in, final int seekPos) throws IOException {
    in.seek(seekPos);
    assertEquals(CHUNK[seekPos % CHUNK.length], in.readByte());
  }

  @Test
  public void testSeek() throws IOException {
    final FSDataInputStream in = surfFs.open(new Path(LONG_FILE_PATH));
    assertEquals(CHUNK[0], in.readByte());

    // Test seek forward
    assertSeekThenReadEqualsChunk(in, 1);
    // Test seek forward to last byte in block
    assertSeekThenReadEqualsChunk(in, DFS_BLOCK_SIZE_VALUE -1);
    // Test seek backward
    assertSeekThenReadEqualsChunk(in, 12);
    // Test seek backward (after a seek backward)
    assertSeekThenReadEqualsChunk(in, 9);
    // Test seek forward across block boundaries to first byte in next block
    assertSeekThenReadEqualsChunk(in, DFS_BLOCK_SIZE_VALUE);
    // Test seek backward across block boundaries
    assertSeekThenReadEqualsChunk(in, 12);

    // Test seek to last byte
    assertSeekThenReadEqualsChunk(in, LONG_FILE_NUM_CHUNKS * CHUNK.length - 1);
    // Test seek past last byte (EOF)
    try {
      in.seek(LONG_FILE_NUM_CHUNKS * CHUNK.length);
      fail("Should throw IOException");
    } catch (IOException e) {
      // passed
    } catch (Exception e) {
      fail("Should throw IOException, instead threw "+e);
    }
    try {
      in.seek(LONG_FILE_NUM_CHUNKS * CHUNK.length + 1000);
      fail("Should throw IOException");
    } catch (IOException e) {
      // passed
    } catch (Exception e) {
      fail("Should throw IOException, instead threw "+e);
    }

    // Test seek after EOFException
    assertSeekThenReadEqualsChunk(in, DFS_BLOCK_SIZE_VALUE + 8);
    // Test seek backward across block boundaries (after EOFException)
    assertSeekThenReadEqualsChunk(in, 2);
  }

  /**
   * The contract for all read(...) methods is that all bytes that can be read, up to length,
   * will be read. If the read starts at or beyond the end-of-file, then -1 should be returned.
   * This is a rather confusing contract, so we test out the variations here.
   */
  @Test
  public void testReadEOF() throws IOException {
    final FSDataInputStream in = surfFs.open(new Path(LONG_FILE_PATH));
    final int EOF = -1;
    final byte bufSize = 16;
    final byte[] buf = new byte[bufSize];

    /* Test InputStream.read(byte[], offset, length) */

    // Seek to last byte then read it
    assertSeekThenReadEqualsChunk(in, LONG_FILE_NUM_CHUNKS * CHUNK.length - 1);
    // Test next read returns -1
    assertEquals(EOF, in.read());
    assertEquals(EOF, in.read(buf, 0, buf.length));

    // Seek to last byte
    in.seek(LONG_FILE_NUM_CHUNKS * CHUNK.length - 1);
    // Read should read up to the last byte
    assertEquals(1, in.read(buf, 0, buf.length));
    // Further read should return -1 because we are starting past EOF
    assertEquals(EOF, in.read(buf, 0, buf.length));
    // The position should only get updated up to the EOF point
    assertEquals(LONG_FILE_NUM_CHUNKS * CHUNK.length, in.getPos());
    // Read should only read as much as the buffer length
    in.seek(1);
    assertEquals(buf.length, in.read(buf, 0, buf.length));

    /* Test PositionedReadable read(position, byte[], offset, length) */
    in.seek(0);
    // Read should read up to the last byte
    assertEquals(1, in.read(LONG_FILE_NUM_CHUNKS * CHUNK.length - 1, buf, 0, buf.length));
    assertEquals(0, in.getPos());
    // Read should return -1 because we are starting past EOF
    assertEquals(EOF, in.read(LONG_FILE_NUM_CHUNKS * CHUNK.length, buf, 0, buf.length));
    assertEquals(0, in.getPos());
    // Read should only read as much as the buffer length
    assertEquals(buf.length, in.read(1, buf, 0, buf.length));
    assertEquals(0, in.getPos());
  }

  /**
   * The contract for readFully(...) methods is different from read(...). It should return IOException
   * when trying to read across the EOF.
   * Also, it should never update the position.
   */
  @Test
  public void testReadFullyEOF() throws IOException {
    final FSDataInputStream in = surfFs.open(new Path(LONG_FILE_PATH));
    final int fileSize = LONG_FILE_NUM_CHUNKS * CHUNK.length;

    // readFully from start of file
    final byte[] bufFromStart = new byte[fileSize];
    in.readFully(0, bufFromStart);
    assertBufEqualsChunk(bufFromStart, 0, fileSize);
    assertEquals(0, in.getPos());

    // readFully from offset at first block
    final byte[] bufFromFirstBlock = new byte[fileSize-1];
    in.readFully(1, bufFromFirstBlock);
    assertBufEqualsChunk(bufFromFirstBlock, 1, fileSize - 1);
    assertEquals(0, in.getPos());

    // readFully from offset at second block
    final byte[] bufFromSecondBlock = new byte[fileSize-(DFS_BLOCK_SIZE_VALUE+2)];
    in.readFully(DFS_BLOCK_SIZE_VALUE+2, bufFromSecondBlock);
    assertBufEqualsChunk(bufFromSecondBlock, DFS_BLOCK_SIZE_VALUE+2, fileSize - (DFS_BLOCK_SIZE_VALUE+2));
    assertEquals(0, in.getPos());

    // readFully with length, to end of file
    Arrays.fill(bufFromFirstBlock, (byte) 0);
    in.readFully(1, bufFromFirstBlock, 0, fileSize - 1);
    assertBufEqualsChunk(bufFromFirstBlock, 1, fileSize -1);
    assertEquals(0, in.getPos());

    // readFully past end of file
    Arrays.fill(bufFromStart, (byte) 0);
    try {
      in.readFully(1, bufFromStart, 0, fileSize);
      fail("Should throw EOF");
    } catch (EOFException e) {
      // passed
    } catch (Exception e) {
      fail("Should throw EOF, instead threw " + e);
    }
    assertEquals(0, in.getPos());
  }

  /**
   * Read from the wrong Surf address. This should still succeed, by falling back to the base FS.
   */
  @Test
  public void testFallbackRead() throws IOException {
    final String wrongAddress = "localhost:18888";

    final SurfFS surfFsWithWrongAddress = new SurfFS();
    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());
    surfFsWithWrongAddress.initialize(URI.create(SURF+"://"+wrongAddress), conf);

    read(surfFsWithWrongAddress, SHORT_FILE_PATH, SHORT_FILE_NUM_CHUNKS);
    read(surfFsWithWrongAddress, LONG_FILE_PATH, LONG_FILE_NUM_CHUNKS);
  }

  /**
   * Copy with the wrong Surf port. This should still succeed, by falling back to the base FS.
   */
  @Test
  public void testFallbackCopy() throws IOException {
    final String wrongAddress = "localhost:18888";

    final SurfFS surfFsWithWrongAddress = new SurfFS();
    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());
    surfFsWithWrongAddress.initialize(URI.create(SURF+"://"+wrongAddress), conf);

    copyBytes(surfFsWithWrongAddress, SHORT_FILE_PATH, SHORT_FILE_NUM_CHUNKS);
    copyBytes(surfFsWithWrongAddress, LONG_FILE_PATH, LONG_FILE_NUM_CHUNKS);
  }

  /**
   * Operations should fail when fallback is turned off
   */
  @Test
  public void testMetaServerWithoutFallback() throws IOException {
    final String wrongAddress = "localhost:18888";

    final SurfFS surfFsWithoutFallback = new SurfFS();
    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());
    conf.setBoolean(SurfFS.FALLBACK_KEY, false);
    surfFsWithoutFallback.initialize(URI.create(SURF + "://" + wrongAddress), conf);

    try {
      final FSDataInputStream in = surfFsWithoutFallback.open(new Path(SHORT_FILE_PATH));
      fail("Should throw IOException");
    } catch (final IOException e) {
      // passed
    } catch (Exception e) {
      fail("Should throw IOException, instead threw " + e);
    }
  }

  /**
   * Test that the wrapped stream is of correct type given configuration
   */
  @Test
  public void testCacheserverWithoutFallback() throws IOException {
    // On default, should recieve an instance of FallbackFSInputStream
    final FSDataInputStream in = surfFs.open(new Path(SHORT_FILE_PATH));
    assertTrue(in.getWrappedStream() instanceof FallbackFSInputStream);

    // When FALLBACK_CACHESERVER_KEY is set to false, should not receive an instance of FallbackFSInputStream
    final SurfFS surfFsWithoutFallback = new SurfFS();
    final Configuration conf = new Configuration();
    conf.set(SurfFS.BASE_FS_ADDRESS_KEY, baseFs.getUri().toString());
    conf.setBoolean(SurfFS.FALLBACK_KEY, false);
    surfFsWithoutFallback.initialize(URI.create(SURF + "://" + SURF_ADDRESS), conf);

    final FSDataInputStream inWithNoFallback = surfFsWithoutFallback.open(new Path(SHORT_FILE_PATH));
    assertFalse(inWithNoFallback.getWrappedStream() instanceof FallbackFSInputStream);
    assertTrue(inWithNoFallback.getWrappedStream() instanceof SurfFSInputStream);
  }
}
