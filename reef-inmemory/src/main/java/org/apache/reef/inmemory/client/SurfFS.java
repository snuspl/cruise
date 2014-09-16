package org.apache.reef.inmemory.client;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.service.SurfMetaService;
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
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides a transparent caching layer on top of a base FS (e.g. HDFS).
 * Surf can be configured for access under the surf:// scheme by Hadoop FileSystem-compatible
 * tools and frameworks, by setting the following:
 *   fs.defaultFS: the driver's address (e.g., surf://localhost:18000, surf://yarn.reef-job-InMemory)
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

  public static final String CACHECLIENT_RETRIES_KEY = "surf.cache.client.retries";
  public static final int CACHECLIENT_RETRIES_DEFAULT = 3;

  public static final String CACHECLIENT_RETRIES_INTERVAL_MS_KEY = "surf.cache.client.retries.interval.ms";
  public static final int CACHECLIENT_RETRIES_INTERVAL_MS_DEFAULT = 500;

  public static final String CACHECLIENT_BUFFER_SIZE_KEY = "surf.cache.client.buffer.size";
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

  /**
   * Return current Client, or lazy load if not assigned.
   * Throws runtime exception if Client cannot be found, because we cannot make progress without a Client.
   */
  private SurfMetaService.Client getMetaClient() {

    if (this.metaClient != null) {
      return this.metaClient;
    } else {
      try {
        LOG.log(Level.INFO, "Connecting to metaserver at {0}", metaserverAddress);
        HostAndPort metaAddress = HostAndPort.fromString(metaserverAddress);

        TTransport transport = new TFramedTransport(new TSocket(metaAddress.getHostText(), metaAddress.getPort()));
        transport.open();
        TProtocol protocol = new TMultiplexedProtocol(
                new TCompactProtocol(transport),
                SurfMetaService.class.getName());
        this.metaClient = new SurfMetaService.Client(protocol);
        return this.metaClient;
      } catch (TTransportException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void initialize(final URI uri,
                         final Configuration conf) throws IOException {
    super.initialize(uri, conf);
    String baseFsAddress = conf.get(BASE_FS_ADDRESS_KEY, BASE_FS_ADDRESS_DEFAULT);
    this.uri = uri;
    this.baseFsUri = URI.create(baseFsAddress);
    this.baseFs = new DistributedFileSystem();
    this.baseFs.initialize(this.baseFsUri, conf);
    this.setConf(conf);

    this.metaserverAddress = getMetaserverResolver().getAddress();
    LOG.log(Level.FINE, "SurfFs address resolved to: "+this.metaserverAddress);
    this.cacheClientManager = new CacheClientManager(
            conf.getInt(CACHECLIENT_RETRIES_KEY, CACHECLIENT_RETRIES_DEFAULT),
            conf.getInt(CACHECLIENT_RETRIES_INTERVAL_MS_KEY, CACHECLIENT_RETRIES_INTERVAL_MS_DEFAULT),
            conf.getInt(CACHECLIENT_BUFFER_SIZE_KEY, CACHECLIENT_BUFFER_SIZE_DEFAULT));
  }

  /**
   * Get the MetaserverResolver based on the provided uri
   */
  public MetaserverResolver getMetaserverResolver() {
    final String address = uri.getAuthority();

    if (address.startsWith("yarn.")) {
      return new YarnMetaserverResolver(address, getConf());
    } else {
      return new InetMetaserverResolver(address);
    }
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
   * Note: calling open triggers a load on the file, if it's not yet in Surf
   */
  @Override
  public synchronized FSDataInputStream open(Path path, final int bufferSize) throws IOException {
    LOG.log(Level.INFO, "Open called on {0}, using {1}",
            new String[]{path.toString(), path.toUri().getPath().toString()});

    try {
      FileMeta metadata = getMetaClient().getFileMeta(path.toUri().getPath());
      return new FSDataInputStream(new SurfFSInputStream(metadata, cacheClientManager, getConf()));
    } catch (org.apache.reef.inmemory.common.exceptions.FileNotFoundException e) {
      LOG.log(Level.FINE, "FileNotFoundException ", e);
      throw new FileNotFoundException(e.getMessage());
    } catch (TException e) {
      LOG.log(Level.SEVERE, "TException", e);
      throw new IOException(e);
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

  private BlockLocation getBlockLocation(List<NodeInfo> locations, long start, long len) {
    final String[] addresses = new String[locations.size()];
    final String[] hosts = new String[locations.size()];
    final String[] racks = new String[locations.size()];

    int idx = 0;
    for (NodeInfo location : locations) {
      addresses[idx] = location.getAddress();
      hosts[idx] = HostAndPort.fromString(location.getAddress()).getHostText();
      racks[idx] = location.getRack();
      idx++;
    }

    return new BlockLocation(addresses, hosts, racks, start, len);
  }

  /**
   * Note: calling getFileBlockLocations triggers a pre-load on the file, if it's not yet in Surf
   * @return The {@code BlockLocation}s of blocks containing file. It returns an empty array if the file size is 0.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {

    LOG.log(Level.INFO, "getFileBlockLocations called on {0}, using {1}",
            new String[]{file.getPath().toString(), file.getPath().toUri().getPath().toString()});

    List<BlockLocation> blockLocations = new LinkedList<>();

    try {
      final FileMeta metadata = getMetaClient().getFileMeta(file.getPath().toUri().getPath());
      long startRemaining = start;
      Iterator<BlockInfo> iter = metadata.getBlocksIterator();
      // HDFS returns empty array with the file of size 0(e.g. _SUCCESS file from Map/Reduce Task)
      if (iter == null) {
        return new BlockLocation[0];
      }

      // Find the block that contains start and add its locations
      while (iter.hasNext()) {
        final BlockInfo block = iter.next();
        startRemaining -= block.getLength();
        if (startRemaining < 0) {
          blockLocations.add(getBlockLocation(block.getLocations(), block.getOffSet(), block.getLength()));
          break;
        }
      }

      // Add locations of blocks after that, up to len
      long lenRemaining = len + startRemaining;
      while (lenRemaining > 0 && iter.hasNext()) {
        final BlockInfo block = iter.next();
        lenRemaining -= block.getLength();
        blockLocations.add(getBlockLocation(block.getLocations(), block.getOffSet(), block.getLength()));
      }

      return blockLocations.toArray(new BlockLocation[blockLocations.size()]);

    } catch (org.apache.reef.inmemory.common.exceptions.FileNotFoundException e) {
      LOG.log(Level.FINE, "FileNotFoundException: "+e+" "+e.getCause());
      throw new FileNotFoundException(e.getMessage());
    } catch (TException e) {
      LOG.log(Level.SEVERE, "TException: "+e+" "+e.getCause());
      throw new IOException(e.getMessage());
    }
  }
}
