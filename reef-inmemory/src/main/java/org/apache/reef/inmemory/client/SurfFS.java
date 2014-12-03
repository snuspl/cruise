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
import org.apache.reef.inmemory.common.exceptions.FileNotFoundException;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

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
 * Surf supports create, append, rename, delete, mkdirs
 * delegating them to the base FS
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
  private String metaserverAddress;

  private Configuration conf;
  private URI uri;
  private URI baseFsUri;

  private final MetaClientManager metaClientManager;

  public SurfFS() {
    this.metaClientManager = new MetaClientManagerImpl();
  }

  /**
   * Only used for testing
   */
  protected SurfFS(final FileSystem baseFs,
                   final MetaClientManager metaClientManager) {
    this.baseFs = baseFs;
    this.metaClientManager = metaClientManager;
  }

  /**
   * Instantiate and return a new MetaClient for thread-safety
   */
  public SurfMetaService.Client getMetaClient() throws IOException {
    try {
      return this.metaClientManager.get(this.metaserverAddress);
    } catch (TTransportException e) {
      throw new IOException(e);
    }
  }

  /**
   * Instantiate and return a new CacheClientManager for thread-safety
   */
  public CacheClientManager getCacheClientManager() {
    return new CacheClientManagerImpl(
        this.conf.getInt(CACHECLIENT_RETRIES_KEY, CACHECLIENT_RETRIES_DEFAULT),
        this.conf.getInt(CACHECLIENT_RETRIES_INTERVAL_MS_KEY, CACHECLIENT_RETRIES_INTERVAL_MS_DEFAULT),
        this.conf.getInt(CACHECLIENT_BUFFER_SIZE_KEY, CACHECLIENT_BUFFER_SIZE_DEFAULT));
  }

  @Override
  public void initialize(final URI uri,
                         final Configuration conf) throws IOException {
    super.initialize(uri, conf);
    final String baseFsAddress = conf.get(BASE_FS_ADDRESS_KEY, BASE_FS_ADDRESS_DEFAULT);
    this.uri = uri;
    this.baseFsUri = URI.create(baseFsAddress);
    this.baseFs = new DistributedFileSystem();
    this.baseFs.initialize(this.baseFsUri, conf);
    this.setConf(conf);
    this.conf = conf;

    this.metaserverAddress = getMetaserverResolver().getAddress();
    LOG.log(Level.FINE, "SurfFs address resolved to: "+this.metaserverAddress);
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
    final URI basePathUri = baseFsPath.toUri();
    if (basePathUri.isAbsolute()) {
      return new Path(uri.getScheme(), uri.getAuthority(), basePathUri.getPath());
    } else {
      return baseFsPath;
    }
  }

  protected Path pathToBase(final Path surfPath) {
    final URI surfPathUri = surfPath.toUri();
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
  public FSDataInputStream open(Path path, final int bufferSize) throws IOException {
    LOG.log(Level.INFO, "Open called on {0}, using {1}",
      new Object[]{path, path.toUri().getPath()});

    try {
      final FileMeta metadata = getMetaClient().getFileMeta(path.toUri().getPath());
      final CacheClientManager cacheClientManager = getCacheClientManager();
      return new FSDataInputStream(new SurfFSInputStream(metadata, cacheClientManager, getConf()));
    } catch (org.apache.reef.inmemory.common.exceptions.FileNotFoundException e) {
      LOG.log(Level.FINE, "FileNotFoundException ", e);
      throw new java.io.FileNotFoundException(e.getMessage());
    } catch (TException e) {
      LOG.log(Level.SEVERE, "TException", e);
      throw new IOException(e);
    }
  }

  /*
   * Methods related to write are delegated
   * to the Base FS (They will be implemented later)
   */
  @Override
  public FSDataOutputStream create(Path path, FsPermission permission, boolean overwrite, int bufferSize,
                                   short replication, long blockSize, Progressable progress) throws IOException {
    final String decodedPath = path.toUri().getPath();
    SurfMetaService.Client metaClient = getMetaClient();
    final CacheClientManager cacheClientManager = getCacheClientManager();
    try {
      metaClient.create(decodedPath, blockSize);
      return new FSDataOutputStream(new SurfFSOutputStream(decodedPath, metaClient, cacheClientManager, blockSize), new Statistics("surf"));
    } catch (TException e) {
      throw new IOException("Failed to create a file in " + decodedPath, e);
    }
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    return baseFs.append(pathToBase(f), bufferSize, progress);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    return baseFs.rename(pathToBase(src), pathToBase(dst));
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    return baseFs.delete(pathToBase(f), recursive);
  }

  @Override
  public FileStatus[] listStatus(Path path) throws IOException {
    final FileStatus[] statuses = baseFs.listStatus(pathToBase(path));
    for (final FileStatus status : statuses) {
      setStatusToSurf(status);
    }
    return statuses;
  }

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
    return baseFs.mkdirs(pathToBase(path), fsPermission);
  }

  @Override
  public FileStatus getFileStatus(Path path) throws IOException {
    final Path absolutePath = fixRelativePart(path);
    try {
      final FileMeta meta = getMetaClient().getFileMeta(absolutePath.toUri().getPath());
      return getFileStatusFromMeta(meta);
    } catch (FileNotFoundException e) {
      throw new java.io.FileNotFoundException("File not found in the meta server");
    } catch (TException e) {
      throw new IOException ("Failed to get File Status", e);
    }
  }

  private BlockLocation getBlockLocation(List<NodeInfo> locations, long start, long len) {
    final String[] addresses = new String[locations.size()];
    final String[] hosts = new String[locations.size()];
    final String[] topologyPaths = new String[locations.size()];

    int idx = 0;
    for (NodeInfo location : locations) {
      addresses[idx] = location.getAddress();
      hosts[idx] = HostAndPort.fromString(location.getAddress()).getHostText();
      topologyPaths[idx] = location.getRack() + "/" + location.getAddress();
      LOG.log(Level.INFO, "BlockLocation: "+addresses[idx]+", "+hosts[idx]+", "+topologyPaths[idx]);
      idx++;
    }

    return new BlockLocation(addresses, hosts, topologyPaths, start, len);
  }

  /**
   * Note: calling getFileBlockLocations triggers a pre-load on the file, if it's not yet in Surf
   * @return The {@code BlockLocation}s of blocks containing file. It returns an empty array if the file size is 0.
   */
  @Override
  public BlockLocation[] getFileBlockLocations(FileStatus file, long start, long len) throws IOException {

    LOG.log(Level.INFO, "getFileBlockLocations called on {0}, using {1}",
      new Object[]{file.getPath(), file.getPath().toUri().getPath()});

    final List<BlockLocation> blockLocations = new LinkedList<>();

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

      LOG.log(Level.INFO, "Block locations size: "+blockLocations.size());

      return blockLocations.toArray(new BlockLocation[blockLocations.size()]);

    } catch (FileNotFoundException e) {
      LOG.log(Level.FINE, "FileNotFoundException: "+e+" "+e.getCause());
      throw new java.io.FileNotFoundException(e.getMessage());
    } catch (TException e) {
      LOG.log(Level.SEVERE, "TException: "+e+" "+e.getCause());
      throw new IOException(e.getMessage());
    }
  }

  /**
   * Translate File Meta received from the meta server
   * to File Status used in FileSystem API.
   */
  private FileStatus getFileStatusFromMeta(final FileMeta meta) {
    if (meta == null) {
      return null;
    } else {
      final long length = meta.getFileSize();
      final boolean isDir = meta.isDirectory();
      final int replication = -1;
      final long blockSize = meta.getBlockSize();
      final long modificationTime = -1;
      final Path path = new Path(meta.getFullPath());
      // TODO FsPermission, String owner, String group, Path symlink/
      return new FileStatus(length, isDir, replication, blockSize, modificationTime, path);
    }
  }
}
