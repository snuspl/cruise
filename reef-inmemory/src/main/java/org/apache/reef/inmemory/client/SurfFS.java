package org.apache.reef.inmemory.client;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.exceptions.BlockLoadingException;
import org.apache.reef.inmemory.fs.exceptions.BlockNotFoundException;
import org.apache.reef.inmemory.fs.service.SurfCacheService;
import org.apache.reef.inmemory.fs.service.SurfMetaService;
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
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides a transparent caching layer on top of a base FS (e.g. HDFS).
 * Surf can be configured for access under the surf:// scheme by Hadoop FileSystem-compatible
 * tools and frameworks, by setting the following:
 *   fs.defaultFS: the driver's address (e.g., surf://localhost:9001)
 *   fs.surf.impl: this class (org.apache.reef.inmemory.client.SurfFS)
 *   surf.basefs: base FS address (e.g., hdfs://localhost:9000)
 *
 * SurfFS is a read-only filesystem. Thus,
 *   create, append, rename, delete, mkdirs
 * throw an UnsupportedOperationException
 */
public final class SurfFS extends FileSystem {

  public static final String BASE_FS_ADDRESS_KEY = "surf.basefs";
  public static final String BASE_FS_ADDRESS_DEFAULT = "hdfs://localhost:9000";

  public static final String METASERVER_ADDRESS_KEY = "surf.metaserver.address";
  public static final String METASERVER_ADDRESS_DEFAULT = "localhost:18000";

  public static final String CACHESERVER_RETRIES_KEY = "surf.cacheserver.retries";
  public static final int CACHESERVER_RETRIES_DEFAULT = 3;

  public static final String CACHESERVER_RETRIES_INTERVAL_MS_KEY = "surf.cacheserver.retries.interval.ms";
  public static final int CACHESERVER_RETRIES_INTERVAL_MS_DEFAULT = 500;

  private static final Logger LOG = Logger.getLogger(SurfFS.class.getName());

  // These cannot be final, because the empty constructor + intialize() are called externally
  private FileSystem baseFs;
  private SurfMetaService.Client metaClient;
  private final Map<String, SurfCacheService.Client> cacheClients = new HashMap<>();

  private String metaserverAddress;
  private int cacheserverRetries;
  private int cacheserverRetriesInterval;

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

  private static SurfCacheService.Client getTaskClient(String address)
          throws TTransportException {
    HostAndPort taskAddress = HostAndPort.fromString(address);

    TTransport transport = new TFramedTransport(new TSocket(taskAddress.getHostText(), taskAddress.getPort()));
    transport.open();
    TProtocol protocol = new TMultiplexedProtocol(
            new TCompactProtocol(transport),
            SurfCacheService.class.getName());
    return new SurfCacheService.Client(protocol);
  }

  @Override
  public void initialize(final URI uri,
                         final Configuration conf) throws IOException {
    super.initialize(uri, conf);

    metaserverAddress = conf.get(METASERVER_ADDRESS_KEY, METASERVER_ADDRESS_DEFAULT);

    cacheserverRetries = conf.getInt(CACHESERVER_RETRIES_KEY, CACHESERVER_RETRIES_DEFAULT);
    cacheserverRetriesInterval = conf.getInt(
            CACHESERVER_RETRIES_INTERVAL_MS_KEY, CACHESERVER_RETRIES_INTERVAL_MS_DEFAULT);

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

  /**
   * Includes retry on BlockLoadingException.
   *
   * In this implementation, retry is also done on BlockNotFoundException,
   * because Driver does not wait until Task confirmation that block loading has been initiated.
   * This should be fixed on Driver-Task communication side.
   */
  private ByteBuffer getBlockBuffer(BlockInfo block) throws TException {
    String location = block.getLocationsIterator().next();

    if (!cacheClients.containsKey(location)) {
      LOG.log(Level.INFO, "Connecting to cache service at: "+location);
      cacheClients.put(location, getTaskClient(location));
      LOG.log(Level.INFO, "Connected! to cache service at: "+location);
    }
    SurfCacheService.Client client = cacheClients.get(location);

    LOG.log(Level.INFO, "Sending block request: "+block);
    for (int i = 0; i < 1 + cacheserverRetries; i++) {
      try {
        return client.getData(block);
      } catch (BlockLoadingException e) {
        if (i < cacheserverRetries) {
          LOG.log(Level.FINE, "BlockLoadingException, load started: "+e.getTimeStarted());
          try {
            Thread.sleep(cacheserverRetriesInterval);
          } catch (InterruptedException ie) {
            LOG.log(Level.WARNING, "Sleep interrupted: "+ie);
          }
        }
      } catch (BlockNotFoundException e) {
        if (i < cacheserverRetries) {
          LOG.log(Level.FINE, "BlockNotFoundException at "+System.currentTimeMillis());
          try {
            Thread.sleep(cacheserverRetriesInterval);
          } catch (InterruptedException ie) {
            LOG.log(Level.WARNING, "Sleep interrupted: "+ie);
          }
        }
      }
    }
    LOG.log(Level.WARNING, "Exception even after "+cacheserverRetries+" retries. Aborting.");
    throw new BlockNotFoundException();
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
      final List<ByteBuffer> blockBuffers = new ArrayList<>(metadata.getBlocksSize());
      for (BlockInfo block : metadata.getBlocks()) {
        LOG.log(Level.INFO, "Retrieve block: {0}", block);
        blockBuffers.add(getBlockBuffer(block));
      }
      return new FSDataInputStream(new SurfFSInputStream(metadata, blockBuffers));
    } catch (org.apache.reef.inmemory.fs.exceptions.FileNotFoundException e) {
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
