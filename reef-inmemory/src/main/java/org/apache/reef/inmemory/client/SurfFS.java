package org.apache.reef.inmemory.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.reef.inmemory.fs.service.SurfMetaService;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.logging.Logger;

/**
 * Provides a transparent caching layer on top of a base FS (e.g. HDFS).
 * Surf can be configured for access under the surf:// scheme by Hadoop FileSystem-compatible
 * tools and frameworks, by setting the following:
 *   fs.defaultFS: the driver's address (e.g., surf://localhost:9001)
 *   fs.surf.impl: this class (org.apache.reef.inmemory.client.CachedFS)
 *   surf.basefs: base FS address (e.g., hdfs://localhost:9000)
 *
 * In the current design, SurfFS is a read-only filesystem. Thus,
 *   create, append, rename, delete, mkdirs
 * throw an UnsupportedOperationException
 */
public class SurfFS extends FileSystem {

  public static final String BASE_FS_ADDRESS_KEY = "surf.basefs";
  public static final String BASE_FS_ADDRESS_DEFAULT = "hdfs://localhost:9000";

  private static final Logger LOG = Logger.getLogger(SurfFS.class.getName());

  // These cannot be final, because the empty constructor is used externally
  private FileSystem baseFs;
  private SurfMetaService.Client thriftClient;

  private URI uri;
  private URI baseFsUri;

  public SurfFS() {
  }

  protected SurfFS(final FileSystem baseFs,
                   final SurfMetaService.Client thriftClient) {
    this.baseFs = baseFs;
    this.thriftClient = thriftClient;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);

    String baseFsAddress = conf.get(BASE_FS_ADDRESS_KEY, BASE_FS_ADDRESS_DEFAULT);

    this.uri = uri;
    this.baseFsUri = URI.create(baseFsAddress);

    this.baseFs = new DistributedFileSystem();
    this.baseFs.initialize(this.baseFsUri, conf);
  }

  protected Path pathToSurf(Path baseFsPath) {
    URI basePathUri = baseFsPath.toUri();
    if (basePathUri.isAbsolute()) {
      return new Path(uri.getScheme(), uri.getAuthority(), basePathUri.getPath());
    } else {
      return baseFsPath;
    }
  }

  protected Path pathToBase(Path surfPath) {
    URI surfPathUri = surfPath.toUri();
    if (surfPathUri.isAbsolute()) {
      return new Path(baseFsUri.getScheme(), baseFsUri.getAuthority(), surfPathUri.getPath());
    } else {
      return surfPath;
    }
  }

  protected void setStatusToSurf(FileStatus status) {
    status.setPath(
            pathToSurf(status.getPath()));
  }

  @Override
  public String getScheme() {
    return uri.getScheme();
  }

  @Override
  public URI getUri() {
    return uri;
  }

  // TODO: implement open, using thriftClient
  @Override
  public FSDataInputStream open(Path path, final int bufferSize) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream create(Path path, FsPermission fsPermission, boolean b, int i, short i2, long l, Progressable progressable) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(Path path, int i, Progressable progressable) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path path, Path path2) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(Path path, boolean b) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus[] listStatus(Path path) throws FileNotFoundException, IOException {
    FileStatus[] statuses = baseFs.listStatus(pathToBase(path));
    for (FileStatus status : statuses) {
      setStatusToSurf(status);
    }
    return statuses;
  }

  /**
   * Working directory management is delegated to the Base FS
   */
  @Override
  public void setWorkingDirectory(Path path) {
    baseFs.setWorkingDirectory(pathToBase(path));
  }

  @Override
  public Path getWorkingDirectory() {
    return pathToSurf(baseFs.getWorkingDirectory());
  }

  @Override
  public boolean mkdirs(Path path, FsPermission fsPermission) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    FileStatus status = baseFs.getFileStatus(pathToBase(path));
    setStatusToSurf(status);
    return status;
  }
}
