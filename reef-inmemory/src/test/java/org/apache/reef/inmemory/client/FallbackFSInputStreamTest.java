package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

import static org.mockito.Matchers.any;
import static org.mockito.Mockito.*;

/**
 * Verify that an InputStream for FallbackFS is opened and accessed as
 * expected on Exception.
 */
public final class FallbackFSInputStreamTest {

  private Path path;
  private FSInputStream originalIn;
  private FileSystem fallbackFs;
  private FSDataInputStream fallbackIn;
  private FSInputStream fallbackWrappedIn;
  private FSInputStream in;

  private void initializeMocks() throws IOException {
    originalIn = mock(FSInputStream.class);
    fallbackFs = mock(FileSystem.class);
    fallbackIn = mock(FSDataInputStream.class);
    when(fallbackFs.open(any(Path.class))).thenReturn(fallbackIn);
    fallbackWrappedIn = mock(FSInputStream.class);
    when(fallbackIn.getWrappedStream()).thenReturn(fallbackWrappedIn);
    in = new FallbackFSInputStream(originalIn, path, fallbackFs);
  }

  /**
   * Verify that FallbackFS open was called exactly once.
   */
  private void verifyFallbackFsOpen() throws IOException {
    verify(fallbackFs).open(path);
    verify(fallbackIn).getWrappedStream();
  }

  @Before
  public void setUp() throws IOException {
    path = new Path("/test/path");
    initializeMocks();
  }

  private static IOException originalInIOException() {
    return new IOException("originalIn mock Exception");
  }

  /****
   * START
   * Tests of correct wiring: Test that on IOException, the FallbackFs is opened, and the corresponding
   * method is run on the Fallback FS.
   ****/

  /*** Override abstract methods ***/

  @Test
  public void testFallbackSeek() throws IOException {
    final long pos = 1;

    doThrow(originalInIOException()).when(originalIn).seek(pos);
    in.seek(pos);
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).seek(pos);
  }

  @Test
  public void testFallbackGetPos() throws IOException {
    doThrow(originalInIOException()).when(originalIn).getPos();
    in.getPos();
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).getPos();
  }

  @Test
  public void testFallbackSeekToNewSource() throws IOException {
    final long targetPos = 1;

    doThrow(originalInIOException()).when(originalIn).seekToNewSource(targetPos);
    in.seekToNewSource(targetPos);
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).seekToNewSource(targetPos);
  }

  @Test
  public void testFallbackRead1() throws IOException {
    doThrow(originalInIOException()).when(originalIn).read();
    in.read();
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).read();
  }

  /*** Override methods directly inherited from FSInputStream ***/

  @Test
  public void testFallbackRead2() throws IOException {
    final long position = 1;
    final byte[] buffer = new byte[20];
    final int offset = 3;
    final int length = 4;

    doThrow(originalInIOException()).when(originalIn).read(position, buffer, offset, length);
    in.read(position, buffer, offset, length);
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).read(position, buffer, offset, length);
  }

  @Test
  public void testReadFully1() throws IOException {
    final long position = 1;
    final byte[] buffer = new byte[20];
    final int offset = 3;
    final int length = 4;

    doThrow(originalInIOException()).when(originalIn).readFully(position, buffer, offset, length);
    in.readFully(position, buffer, offset, length);
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).readFully(position, buffer, offset, length);
  }

  @Test
  public void testReadFully2() throws IOException {
    final long position = 1;
    final byte[] buffer = new byte[20];

    doThrow(originalInIOException()).when(originalIn).readFully(position, buffer);
    in.readFully(position, buffer);
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).readFully(position, buffer);
  }

  /*** Override methods inherited from InputStream ***/

  @Test
  public void testFallbackRead3() throws IOException {
    final byte[] buffer = new byte[20];

    doThrow(originalInIOException()).when(originalIn).read(buffer);
    in.read(buffer);
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).read(buffer);
  }

  @Test
  public void testFallbackRead4() throws IOException {
    final byte[] buffer = new byte[20];
    final int offset = 3;
    final int length = 4;

    doThrow(originalInIOException()).when(originalIn).read(buffer, offset, length);
    in.read(buffer, offset, length);
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).read(buffer, offset, length);
  }

  @Test
  public void testFallbackSkip() throws IOException {
    final long n = 1;

    doThrow(originalInIOException()).when(originalIn).skip(n);
    in.skip(n);
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).skip(n);
  }

  @Test
  public void testFallbackAvailable() throws IOException {
    doThrow(originalInIOException()).when(originalIn).available();
    in.available();
    verifyFallbackFsOpen();
    verify(fallbackWrappedIn).available();
  }

  /****
   * END
   * Tests of correct wiring: Test that on IOException, the FallbackFs is opened, and the corresponding
   * method is run on the Fallback FS.
   ****/

  /**
   * Test that the number of seeks reaching the original InputStream and the fallback InputStream are as expected:
   * The seek that throws an Exception runs a final seek on the original InputStream and
   * triggers two seeks on the fallback InputStream: first to initialize, second to run the actual seek.
   */
  @Test
  public void testSeek() throws IOException {
    final long EXCEPTION_THROWING_SEEK = Integer.MAX_VALUE;
    doThrow(originalInIOException()).when(originalIn).seek(EXCEPTION_THROWING_SEEK);

    final int numSeeksBeforeException = 5;
    final int numSeeksAfterException = 9;

    for (int i = 0; i < numSeeksBeforeException; i++) {
      in.seek(i);
    }
    // An IOException should route to fallback
    in.seek(EXCEPTION_THROWING_SEEK);
    // After an IOException, ALL calls should go to fallback
    for (int i = 0; i < numSeeksAfterException; i++) {
      in.seek(i);
    }

    // All seeks before the Exception are done on the original InputStream.
    // The seek that throws an Exception runs a final seek on the original InputStream
    verify(originalIn, times(numSeeksBeforeException + 1)).seek(anyInt());
    // The seek that throws an Exception triggers two seeks on the fallback InputStream:
    // first to initialize, second to run the actual seek.
    // After the Exception, all seeks are run at the fallback InputStream
    verify(fallbackWrappedIn, times(numSeeksAfterException + 2)).seek(anyInt());
  }
}
