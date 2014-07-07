package org.apache.reef.inmemory.client;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.reef.inmemory.driver.entity.FileMeta;
import org.apache.reef.inmemory.driver.service.SurfMetaService;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TCompactProtocol;
import org.apache.thrift.protocol.TMultiplexedProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides a transparent caching layer on top of a base FS (e.g. HDFS).
 * Surf can be configured for access under the surf:// scheme by Hadoop FileSystem-compatible
 * tools and frameworks, by setting the following:
 *   driver.defaultFS: the driver's address (e.g., surf://localhost:9001)
 *   driver.surf.impl: this class (org.apache.reef.inmemory.client.SurfFS)
 *   surf.basefs: base FS address (e.g., hdfs://localhost:9000)
 *
 * SurfFS is a read-only filesystem. Thus,
 *   create, append, rename, delete, mkdirs
 * throw an UnsupportedOperationException
 */
public final class SurfFS extends FileSystem {

  public static final String BASE_FS_ADDRESS_KEY = "surf.basefs";
  public static final String BASE_FS_ADDRESS_DEFAULT = "hdfs://localhost:9000";

  public static final String METASERVER_ADDRESS_KEY = "surf.meta.server.address";
  public static final String METASERVER_ADDRESS_DEFAULT = "localhost:18000";

  public static final String CACHECLIENT_RETRIES_KEY = "surf.task.client.retries";
  public static final int CACHECLIENT_RETRIES_DEFAULT = 3;

  public static final String CACHECLIENT_RETRIES_INTERVAL_MS_KEY = "surf.task.client.retries.interval.ms";
  public static final int CACHECLIENT_RETRIES_INTERVAL_MS_DEFAULT = 500;

  public static final String CACHECLIENT_BUFFER_SIZE_KEY = "surf.task.client.buffer.size";
  public static final int CACHECLIENT_BUFFER_SIZE_DEFAULT = 8 * 1024 * 1024;

  private static final Logger LOG = Logger.getLogger(SurfFS.class.getName());

  // These cannot be final, because the empty constructor + intialize() are called externally
  private FileSystem baseFs;
  private SurfMetaService.Client metaClient;
  private CacheClientManager cacheClientManager;

  private String metaserverAddress;

  private URI uri;
  private URI baseFsUri;

  public SurfFS() {
  }

  protected SurfFS(final FileSystem baseFs,
                   final SurfMetaService.Client metaClient) {
    this.baseFs = baseFs;
    this.metaClient = metaClient;
  }

  private static SurfMetaService.Client getMetaClient(String address)
          throws TTransportException {
    HostAndPort metaAddress = HostAndPort.fromString(address);

    TTransport transport = new TFramedTransport(new TSocket(metaAddress.getHostText(), metaAddress.getPort()));
    transport.open();
    TProtocol protocol = new TMultiplexedProtocol(
            new TCompactProtocol(transport),
            SurfMetaService.class.getName());
    return new SurfMetaService.Client(protocol);
  }

  @Override
  public void initialize(final URI uri,
                         final Configuration conf) throws IOException {
    super.initialize(uri, conf);

    metaserverAddress = conf.get(METASERVER_ADDRESS_KEY, METASERVER_ADDRESS_DEFAULT);
    cacheClientManager = new CacheClientManager(
            conf.getInt(CACHECLIENT_RETRIES_KEY, CACHECLIENT_RETRIES_DEFAULT),
            conf.getInt(CACHECLIENT_RETRIES_INTERVAL_MS_KEY, CACHECLIENT_RETRIES_INTERVAL_MS_DEFAULT),
            conf.getInt(CACHECLIENT_BUFFER_SIZE_KEY, CACHECLIENT_BUFFER_SIZE_DEFAULT));

    String baseFsAddress = conf.get(BASE_FS_ADDRESS_KEY, BASE_FS_ADDRESS_DEFAULT);
    this.uri = uri;
    this.baseFsUri = URI.create(baseFsAddress);
    this.baseFs = new DistributedFileSystem();
    this.baseFs.initialize(this.baseFsUri, conf);

    this.setConf(conf);
  }

  protected Path pathToSurf(final Path baseFsPath) {
    URI basePathUri = baseFsPath.toUri();
    if (basePathUri.isAbsolute()) {
      return new Path(uri.getScheme(), uri.getAuthority(), basePathUri.getPath());
    } else {
      return baseFsPath;
    }
  }

  protected Path pathToBase(final Path surfPath) {
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

  @Override
  public synchronized FSDataInputStream open(Path path, final int bufferSize) throws IOException {
    // Lazy loading (for now)
    if (this.metaClient == null) {
      try {
        this.metaClient = getMetaClient(metaserverAddress);
      } catch (TTransportException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      LOG.log(Level.INFO, "getFileMeta called on: "+path+", using: "+path.toUri().getPath());
      FileMeta metadata = metaClient.getFileMeta(path.toUri().getPath());
      return new FSDataInputStream(new SurfFSInputStream(metadata, cacheClientManager));
    } catch (org.apache.reef.inmemory.driver.exceptions.FileNotFoundException e) {
      LOG.log(Level.FINE, "FileNotFoundException: "+e+" "+e.getCause());
      throw new FileNotFoundException(e.getMessage());
    } catch (TException e) {
      LOG.log(Level.SEVERE, "TException: "+e+" "+e.getCause());
      throw new IOException(e.getMessage());
    }
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
