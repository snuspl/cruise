package org.apache.reef.inmemory.client;

import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Wraps an FSInputStream, along with the path used to open the input stream, and
 * a FileSystem to fallback on.
 * If the wrapped input stream throws an IOException (e.g. due to connection lost with
 * a remote host) a new input stream is created with the fallback FileSystem. All calls then
 * go through the fallback input stream.
 */

public final class FallbackFSInputStream extends FSInputStream {

  private static Logger LOG = Logger.getLogger(FallbackFSInputStream.class.getName());

  // The original input stream is always kept, so it can be closed later
  private final FSInputStream originalIn;
  private FSInputStream in;
  private final Path path;
  private final FileSystem fallbackFS;

  private boolean isFallback;
  // The position for the original InputStream is kept in sync locally by updating on methods that change the position.
  // When initializing the fallback InputStream, it is read once to make a seek call.
  // After that, originalPos is updated, but no longer read.
  private long originalPos;

  /**
   * Wrap an input stream created with a known path, to fallback to another FS.
   * @param in The input stream. This will be used until an IOException is encountered.
   * @param path The path used to create the input stream.
   * @param fallbackFS The fallback FS. A new input stream is created using this FS only if the original
   *                   input stream encounters an IOException
   */
  public FallbackFSInputStream(final FSInputStream in, final Path path, final FileSystem fallbackFS) {
    this.originalIn = in;
    this.in = in;
    this.path = path;
    this.fallbackFS = fallbackFS;
    this.isFallback = false;
  }

  private synchronized void initializeFallback(final IOException originalException) throws IOException {
    LOG.log(Level.WARNING, "Fallback in progress, due to exception", originalException);

    if (!this.isFallback) {
      try {
        this.in = (FSInputStream) fallbackFS.open(path).getWrappedStream();
      } catch (final Throwable thrown) {
        LOG.log(Level.WARNING, "Fallback failed with throwable", thrown);
        // Throw the original exception, as fallback failed
        throw originalException;
      }
      this.in.seek(originalPos);
      this.isFallback = true;
    }
  }

  /*** Override abstract methods. ***/

  @Override
  public synchronized void seek(final long pos) throws IOException {
    try {
      in.seek(pos);
      this.originalPos = pos;
    } catch (final IOException e) {
      initializeFallback(e);
      in.seek(pos);
    }
  }

  @Override
  public synchronized long getPos() throws IOException {
    try {
      return in.getPos();
    } catch (final IOException e) {
      initializeFallback(e);
      return in.getPos();
    }
  }

  @Override
  public boolean seekToNewSource(final long targetPos) throws IOException {
    try {
      return in.seekToNewSource(targetPos);
    } catch (final IOException e) {
      initializeFallback(e);
      return in.seekToNewSource(targetPos);
    }
  }

  @Override
  public synchronized int read() throws IOException {
    int numRead;
    try {
      numRead = in.read();
      if (numRead >= 0) {
        this.originalPos += numRead;
      }
    } catch (final IOException e) {
      initializeFallback(e);
      numRead = in.read();
    }
    return numRead;
  }

  /*** Override methods directly inherited from FSInputStream. ***/

  @Override
  public synchronized int read(final long position, final byte[] buffer, final int offset, final int length)
      throws IOException {
    int numRead;
    try {
      numRead = in.read(position, buffer, offset, length);
      if (numRead >= 0) {
        this.originalPos += numRead;
      }
    } catch (final IOException e) {
      initializeFallback(e);
      numRead = in.read(position, buffer, offset, length);
    }
    return numRead;
  }

  @Override
  public void readFully(final long position, final byte[] buffer, final int offset, final int length)
      throws IOException {
    try {
      in.readFully(position, buffer, offset, length);
    } catch (final IOException e) {
      initializeFallback(e);
      in.readFully(position, buffer, offset, length);
    }
  }

  @Override
  public void readFully(final long position, final byte[] buffer) throws IOException {
    try {
      in.readFully(position, buffer);
    } catch (final IOException e) {
      initializeFallback(e);
      in.readFully(position, buffer);
    }
  }

  /*** Override methods inherited from InputStream. ***/

  @Override
  public synchronized int read(final byte[] b) throws IOException {
    int numRead;
    try {
      numRead = in.read(b);
      if (numRead >= 0) {
        this.originalPos += numRead;
      }
    } catch (final IOException e) {
      initializeFallback(e);
      numRead = in.read(b);
    }
    return numRead;
  }

  @Override
  public synchronized int read(final byte[] b, final int off, final int len) throws IOException {
    int numRead;
    try {
      numRead = in.read(b, off, len);
      if (numRead >= 0) {
        this.originalPos += numRead;
      }
    } catch (final IOException e) {
      initializeFallback(e);
      numRead = in.read(b, off, len);
    }
    return numRead;
  }

  @Override
  public long skip(final long n) throws IOException {
    long numSkipped;
    try {
      numSkipped = in.skip(n);
      if (numSkipped >= 0) {
        this.originalPos += numSkipped;
      }
    } catch (final IOException e) {
      initializeFallback(e);
      numSkipped = in.skip(n);
    }
    return numSkipped;
  }

  @Override
  public int available() throws IOException {
    try {
      return in.available();
    } catch (final IOException e) {
      initializeFallback(e);
      return in.available();
    }
  }

  @Override
  public void close() throws IOException {
    in.close();
    if (originalIn != in) {
      try {
        originalIn.close();
      } catch (final IOException e) {
        LOG.log(Level.WARNING, "Close on the original InputStream failed with exception", e);
      }
    }
  }

  /*** Mark/Reset is not supported by FallbackFSInputStream. ***/

  @Override
  public synchronized void mark(final int readlimit) {
  }

  @Override
  public synchronized void reset() throws IOException {
  }

  @Override
  public boolean markSupported() {
    return false;
  }
}
