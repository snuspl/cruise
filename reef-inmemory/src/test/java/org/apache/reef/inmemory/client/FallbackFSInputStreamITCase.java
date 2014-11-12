package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.HdfsConfiguration;
import org.apache.reef.inmemory.common.ITUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Test that the InputStream for FallbackFS is opened and methods are accessed,
 * successfully retrieving the data from HDFS.
 */
public final class FallbackFSInputStreamITCase {

  private static final byte[] CHUNK = new byte[]{(byte)1, (byte)2, (byte)3, (byte)4, (byte)5, (byte)6, (byte)7, (byte)8};
  private static final int DFS_BLOCK_SIZE_VALUE = 512;

  private static final String TESTDIR = ITUtils.getTestDir();

  private static final String FILE_PATH_STR = TESTDIR+"/"+"COUNT.long";
  private static final int FILE_NUM_CHUNKS = 140;

  private static FileSystem fallbackFs;
  private static Path path;

  private FSInputStream originalIn;
  private FSDataInputStream fallbackIn;
  private FSInputStream fallbackWrappedIn;

  private void initializeMocks() throws IOException {
    originalIn = mock(FSInputStream.class);
    fallbackIn = mock(FSDataInputStream.class);
    fallbackWrappedIn = mock(FSInputStream.class);
    when(fallbackIn.getWrappedStream()).thenReturn(fallbackWrappedIn);
  }

  private static void initializeHdfs() throws IOException {
    final Configuration hdfsConfig = new HdfsConfiguration();
    hdfsConfig.setInt(DFSConfigKeys.DFS_REPLICATION_KEY, 3);
    // Reduce blocksize to 512 bytes, to test multiple blocks
    hdfsConfig.setInt(DFSConfigKeys.DFS_BLOCK_SIZE_KEY, DFS_BLOCK_SIZE_VALUE);

    fallbackFs = ITUtils.getHdfs(hdfsConfig);

    path = new Path(FILE_PATH_STR);
    final FSDataOutputStream stream = fallbackFs.create(new Path(FILE_PATH_STR));
    for (int i = 0; i < FILE_NUM_CHUNKS; i++) {
      stream.write(CHUNK);
    }
    stream.close();
  }

  @BeforeClass
  public static void setUpClass() throws IOException {
    initializeHdfs();
  }

  @AfterClass
  public static void tearDownClass() throws IOException {
    fallbackFs.delete(new Path(TESTDIR), true);
  }

  @Before
  public void setUp() throws IOException {
    initializeMocks();
  }

  private static IOException originalInIOException() {
    return new IOException("originalIn mock Exception");
  }

  /****
   * START
   * Tests of read methods: Test that on IOException, the corresponding FallbackFs read is called
   * and reads the expected position, offset, and length.
   ****/

  @Test
  public void testRead1() throws IOException {
    final FSDataInputStream in = new FSDataInputStream(
            new FallbackFSInputStream(originalIn, path, fallbackFs));
    doThrow(originalInIOException()).when(originalIn).read();

    for (int i = 0; i < CHUNK.length; i++) {
      assertEquals(CHUNK[i] & 0xff, in.read());
      assertEquals(i+1, in.getPos());
    }
  }

  private static void assertEqualsChunk(final long position, final byte[] buffer, final int offset, final int numRead) {
    for (int i = 0; i < numRead; i++) {
      assertEquals(CHUNK[(int)((position + i) % CHUNK.length)], buffer[offset + i]);
    }
  }

  @Test
  public void testRead2() throws IOException {
    final long position = 1;
    final byte[] buffer = new byte[20];
    final int offset = 3;
    final int length = 4;

    final FSDataInputStream in = new FSDataInputStream(
            new FallbackFSInputStream(originalIn, path, fallbackFs));
    doThrow(originalInIOException()).when(originalIn).read(position, buffer, offset, length);
    final int numRead = in.read(position, buffer, offset, length);
    assertEquals(length, numRead);
    assertEqualsChunk(position, buffer, offset, numRead);
  }

  @Test
  public void testRead3() throws IOException {
    final byte[] buffer = new byte[20];

    final FSDataInputStream in = new FSDataInputStream(
            new FallbackFSInputStream(originalIn, path, fallbackFs));
    // Throw exception on read(buffer, offset, length), because DataInputStream calls this version
    doThrow(originalInIOException()).when(originalIn).read(buffer, 0, buffer.length);
    final int numRead = in.read(buffer);
    assertEquals(buffer.length, numRead);
    assertEquals(numRead, in.getPos());
    assertEqualsChunk(0, buffer, 0, numRead);
  }

  @Test
  public void testRead4() throws IOException {
    final byte[] buffer = new byte[20];
    final int offset = 3;
    final int length = 4;

    final FSDataInputStream in = new FSDataInputStream(
            new FallbackFSInputStream(originalIn, path, fallbackFs));
    doThrow(originalInIOException()).when(originalIn).read(buffer, offset, length);
    final int numRead = in.read(buffer, offset, length);
    assertEquals(length, numRead);
    assertEquals(numRead, in.getPos());
    assertEqualsChunk(0, buffer, offset, numRead);
  }

  @Test
  public void testReadFully1() throws IOException {
    final long position = 1;
    final byte[] buffer = new byte[20];
    final int offset = 3;
    final int length = 4;

    final FSDataInputStream in = new FSDataInputStream(
            new FallbackFSInputStream(originalIn, path, fallbackFs));
    doThrow(originalInIOException()).when(originalIn).readFully(position, buffer, offset, length);
    in.readFully(position, buffer, offset, length);
    assertEqualsChunk(position, buffer, offset, length);
  }

  @Test
  public void testReadFully2() throws IOException {
    final long position = 1;
    final byte[] buffer = new byte[20];
    final FSDataInputStream in = new FSDataInputStream(
            new FallbackFSInputStream(originalIn, path, fallbackFs));
    // Throw exception on readFully(position, buffer, offset, length), because DataInputStream calls this version
    doThrow(originalInIOException()).when(originalIn).readFully(position, buffer, 0, buffer.length);
    in.readFully(position, buffer);
    assertEqualsChunk(position, buffer, 0, buffer.length);
  }

  /****
   * END
   * Tests of read methods: Test that on IOException, the corresponding FallbackFs read is called
   * and reads from and to the expected position.
   ****/

  /**
   * Test that a seek causing an Exception and a subsequent seek
   * correctly position reads, starting at the seeked position
   */
  @Test
  public void testSeekAndRead() throws IOException {
    final long position = 1;
    final byte[] buffer = new byte[20];
    final int offset = 3;
    final int length = 4;

    final FSDataInputStream in = new FSDataInputStream(
            new FallbackFSInputStream(originalIn, path, fallbackFs));
    // Do not throw an exception on the seek
    in.seek(position);
    // Throw an expection on the read
    doThrow(originalInIOException()).when(originalIn).read(buffer, offset, length);
    final int numRead = in.read(buffer, offset, length);
    // The read should start from the previous seek position, even after fallback
    assertEquals(length, numRead);
    assertEquals(position + numRead, in.getPos());
    assertEqualsChunk(position, buffer, offset, numRead);

    final long nextPosition = 11;
    // The new seek should be done directly on the fallback
    in.seek(nextPosition);
    // Exception will be thrown as before
    final int nextNumRead = in.read(buffer, offset, length);
    // The read should start from nextPosition
    assertEquals(length, nextNumRead);
    assertEquals(nextPosition + nextNumRead, in.getPos());
    assertEqualsChunk(nextPosition, buffer, offset, numRead);
  }
}
