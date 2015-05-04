package org.apache.reef.inmemory.client;

import com.google.common.net.HostAndPort;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;
import org.apache.reef.inmemory.common.entity.BlockMeta;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.FileMetaStatus;
import org.apache.reef.inmemory.common.entity.NodeInfo;
import org.apache.reef.inmemory.common.instrumentation.BasicEventRecorder;
import org.apache.reef.inmemory.common.instrumentation.Event;
import org.apache.reef.inmemory.common.instrumentation.EventRecorder;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.InetAddress;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Provides a transparent caching layer on top of a base FS (e.g. HDFS).
 * Surf can be configured for access under the surf:// scheme by Hadoop FileSystem-compatible
 * tools and frameworks, by setting the following:
 *   fs.defaultFS: the driver's address (e.g., surf://localhost:18000, surf://yarn.reef-job-InMemory)
 *   fs.surf.impl: this class (org.apache.reef.inmemory.client.SurfFS)
 *   surf.basefs: base FS address (e.g., hdfs://localhost:9000)
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

  public static final String FALLBACK_KEY = "surf.fallback";
  public static final boolean FALLBACK_DEFAULT = true;

  public static final String INSTRUMENTATION_CLIENT_LOG_LEVEL_KEY = "surf.instrumentation.client.log.level";
  public static final String INSTRUMENTATION_CLIENT_LOG_LEVEL_DEFAULT = "FINE";

  private static final Logger LOG = Logger.getLogger(SurfFS.class.getName());

  // These cannot be final, because the empty constructor + intialize() are called externally
  private EventRecorder RECORD;
  private FileSystem baseFs;
  private String localAddress;
  private String metaserverAddress;

  private URI uri;
  private Path workingDir;
  private URI baseFsUri;

  private boolean isFallback = FALLBACK_DEFAULT;
  private final MetaClientManager metaClientManager;

  public SurfFS() {
    this.metaClientManager = new MetaClientManagerImpl();
  }

  /**
   * Constructor for test cases only. Does not require a Configuration -- do not call initialize().
   * One or more parameters will usually be mocks.
   */
  protected SurfFS(final FileSystem baseFs,
                   final MetaClientManager metaClientManager,
                   final EventRecorder recorder) {
    this.baseFs = baseFs;
    this.metaClientManager = metaClientManager;
    this.RECORD = recorder;
  }

  @Override
  public void initialize(final URI uri,
                         final Configuration conf) throws IOException {
    RECORD = new BasicEventRecorder(
            conf.get(INSTRUMENTATION_CLIENT_LOG_LEVEL_KEY, INSTRUMENTATION_CLIENT_LOG_LEVEL_DEFAULT));
    final Event initializeEvent = RECORD.event("client.initialize", uri.toString()).start();

    super.initialize(uri, conf);
    final String baseFsAddress = conf.get(BASE_FS_ADDRESS_KEY, BASE_FS_ADDRESS_DEFAULT);
    this.uri = uri;
    this.baseFsUri = URI.create(baseFsAddress);
    this.baseFs = new DistributedFileSystem();
    final Event initializeDfsEvent = RECORD.event("client.initialize.dfs", uri.toString()).start();
    this.baseFs.initialize(this.baseFsUri, conf);
    RECORD.record(initializeDfsEvent.stop());
    this.workingDir = toAbsoluteSurfPath(baseFs.getWorkingDirectory());
    this.setConf(conf);

    this.isFallback = conf.getBoolean(FALLBACK_KEY, FALLBACK_DEFAULT);

    final Event resolveAddressEvent = RECORD.event("client.resolve-address", baseFsUri.toString()).start();
    this.metaserverAddress = getMetaserverResolver().getAddress();
    LOG.log(Level.FINE, "SurfFs address resolved to {0}", this.metaserverAddress);
    RECORD.record(resolveAddressEvent.stop());

    // TODO: Works on local and cluster. Will it work across all platforms? (NetUtils gives the wrong address.)
    this.localAddress = InetAddress.getLocalHost().getHostName();

    LOG.log(Level.INFO, "localAddress: {0}",
            localAddress);
    RECORD.record(initializeEvent.stop());
  }

  @Override
  public void close() throws IOException {
    LOG.log(Level.INFO, "Close called");
    super.close();
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
  public void setWorkingDirectory(final Path path) {
    workingDir = path;
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  /**
   * Loads data into Surf from HDFS: Yes
   * Consistency guarantee: Only for the first load (not responsible for changes in HDFS after)
   * Fallback: Yes (The returned FSDataInputStream also supports fallback)
   *
   * @param path to the file
   * @param bufferSize (TODO: make use of this argument)
   * @return {@code FSDataInputStream} which makes use of the FileMeta of the file loaded in Surf
   * @throws IOException
   */
  @Override
  public FSDataInputStream open(final Path path, final int bufferSize) throws IOException {
    final String pathStr = toAbsolutePathInString(path);
    final Event openEvent = RECORD.event("client.open", pathStr).start();
    LOG.log(Level.INFO, "Open called on {0}, using {1}",
            new Object[]{path, pathStr});

    try {
      final FileMeta metadata = getMetaClient().getOrLoadFileMeta(pathStr, localAddress);
      final CacheClientManager cacheClientManager = getCacheClientManager();
      final SurfFSInputStream surfFSInputStream = new SurfFSInputStream(metadata, cacheClientManager, getConf(), RECORD);
      if (isFallback) {
        return new FSDataInputStream(new FallbackFSInputStream(surfFSInputStream, path, baseFs));
      } else {
        return new FSDataInputStream(surfFSInputStream);
      }
    } catch (org.apache.reef.inmemory.common.exceptions.FileNotFoundException e) {
      if (isFallback) {
        LOG.log(Level.WARNING, "Surf FileNotFoundException ", e);
        return baseFs.open(path, bufferSize);
      } else {
        throw new FileNotFoundException(e.getMessage());
      }
    } catch (TException e) {
      if (isFallback) {
        LOG.log(Level.WARNING, "Surf TException", e);
        return baseFs.open(path, bufferSize);
      } else {
        throw new IOException(e);
      }
    } finally {
      RECORD.record(openEvent.stop());
    }
  }

  /**
   * Loads data into Surf from HDFS: No
   * Consistency guarantee: If same metadata exists in both Surf and HDFS, Surf's overwrites HDFS's
   * Fallback: Yes
   *
   * @param path to the file/directory in question
   * @return {@code FileStatus} of the file/directory
   * @throws IOException
   */
  @Override
  public FileStatus getFileStatus(final Path path) throws IOException {
    try {
      final String pathStr = toAbsolutePathInString(path);
      final FileMetaStatus fileMetaStatus = getMetaClient().getFileMetaStatus(pathStr);
      return toFileStatus(fileMetaStatus);
    } catch (org.apache.reef.inmemory.common.exceptions.FileNotFoundException e) {
      if (isFallback) {
        LOG.log(Level.WARNING, "The file is not found in Surf, trying baseFs...", e);
        return baseFs.getFileStatus(toAbsoluteBasePath(path));
      } else {
        throw new java.io.FileNotFoundException("File not found in the meta server");
      }
    } catch (TException e) {
      if (isFallback) {
        LOG.log(Level.WARNING, "Surf TException, trying baseFs...", e);
        return baseFs.getFileStatus(toAbsoluteBasePath(path));
      } else {
        throw new IOException ("Failed to get File Status from Surf", e);
      }
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  /**
   * Loads data into Surf from HDFS: Yes
   * Consistency guarantee: Only for the first load (not responsible for changes in HDFS after)
   * Fallback: Yes
   *
   * @param file of the blocks
   * @param start offset of the block
   * @param len from the start offset
   * @return The {@code BlockLocation}s of blocks containing file. It returns an empty array if the file size is 0.
   * @throws IOException
   */
  @Override
  public BlockLocation[] getFileBlockLocations(final FileStatus file, final long start, final long len) throws IOException {

    LOG.log(Level.INFO, "getFileBlockLocations called on {0}, using {1}",
      new Object[]{file.getPath(), toAbsolutePathInString(file.getPath())});

    final List<BlockLocation> blockLocations = new LinkedList<>();

    try {
      final FileMeta metadata = getMetaClient().getOrLoadFileMeta(toAbsolutePathInString(file.getPath()), localAddress);
      long startRemaining = start;
      final Iterator<BlockMeta> iter = metadata.getBlocksIterator();
      // HDFS returns empty array with the file of size 0(e.g. _SUCCESS file from Map/Reduce Task)
      if (iter == null) {
        return new BlockLocation[0];
      }

      // Find the block that contains start and add its locations
      while (iter.hasNext()) {
        final BlockMeta block = iter.next();
        startRemaining -= block.getLength();
        if (startRemaining < 0) {
          blockLocations.add(getBlockLocation(block.getLocations(), block.getOffSet(), block.getLength()));
          break;
        }
      }

      // Add locations of blocks after that, up to len
      long lenRemaining = len + startRemaining;
      while (lenRemaining > 0 && iter.hasNext()) {
        final BlockMeta block = iter.next();
        lenRemaining -= block.getLength();
        blockLocations.add(getBlockLocation(block.getLocations(), block.getOffSet(), block.getLength()));
      }

      LOG.log(Level.INFO, "Block locations size: "+blockLocations.size());

      return blockLocations.toArray(new BlockLocation[blockLocations.size()]);

    } catch (org.apache.reef.inmemory.common.exceptions.FileNotFoundException e) {
      if (isFallback) {
        LOG.log(Level.WARNING, "FileNotFoundException: ", e);
        return baseFs.getFileBlockLocations(file, start, len);
      } else {
        throw new FileNotFoundException(e.getMessage());
      }
    } catch (TException e) {
      if (isFallback) {
        LOG.log(Level.WARNING, "TException: ", e);
        return baseFs.getFileBlockLocations(file, start, len);
      } else {
        throw new IOException(e);
      }
    }
  }

  /**
   * Loads data into Surf from HDFS: No
   * Consistency guarantee: If same metadata exists in both Surf and HDFS, Surf's overwrites HDFS's
   * Fallback: No (TODO)
   *
   * Path cases
   * 1. Empty directory: returns the filestatus of the directory
   * 2. Non-empty directory: returns the filestatuses of the directory's children
   * 3. File: returns the filestatus of the file
   *
   * @param path to a file or directory
   * @return {@code FileStatus[]} in Surf and HDFS
   * @throws IOException
   */
  @Override
  public FileStatus[] listStatus(final Path path) throws IOException {
    final String pathStr = toAbsolutePathInString(path);
    try {
      final List<FileMetaStatus> fileMetaStatusList = getMetaClient().listFileMetaStatus(pathStr);
      final FileStatus[] fileStatuses = new FileStatus[fileMetaStatusList.size()];
      for (int i = 0; i < fileMetaStatusList.size(); i++) {
        final FileMetaStatus fileMetaStatus = fileMetaStatusList.get(i);
        fileStatuses[i] = (toFileStatus(fileMetaStatus));
      }
      return fileStatuses;
    } catch (TException e) {
      throw new IOException(e);
    } catch (URISyntaxException e) {
      throw new IOException(e);
    }
  }

  @Override
  public FSDataOutputStream create(final Path path, final FsPermission permission, final boolean overwrite, final int bufferSize,
                                   final short baseFsReplication, final long blockSize, final Progressable progress) throws IOException {
    // TODO: handle permission, overwrite, bufferSize, progress
    final String pathStr = toAbsolutePathInString(path);
    try {
      final SurfMetaService.Client metaClient = getMetaClient();
      final CacheClientManager cacheClientManager = getCacheClientManager();
      metaClient.create(pathStr, blockSize, baseFsReplication);
      return new FSDataOutputStream(new SurfFSOutputStream(pathStr, metaClient, cacheClientManager, blockSize), new Statistics("surf"));
    } catch (TException e) {
      throw new IOException("Failed to create a file in " + pathStr, e);
    }
  }

  /**
   * This operation is not yet supported by Surf
   * Also, no fallback procedure is provided
   * @throws UnsupportedOperationException
   */
  @Override
  public FSDataOutputStream append(final Path f, final int bufferSize, final Progressable progress) throws IOException {
    throw new UnsupportedOperationException("Append is not supported yet.");
  }

  @Override
  public boolean mkdirs(final Path path, final FsPermission fsPermission) throws IOException {
    final String pathStr = toAbsolutePathInString(path);
    try {
      return getMetaClient().mkdirs(pathStr);
    } catch (TException e) {
      throw new IOException("Failed to make directory in " + pathStr, e);
    }
  }

  @Override
  public boolean rename(final Path src, final Path dst) throws IOException {
    final String srcPathStr = toAbsolutePathInString(src);
    final String dstPathStr = toAbsolutePathInString(dst);
    try {
      return getMetaClient().rename(srcPathStr, dstPathStr);
    } catch (TException e) {
      throw new IOException("Failed to rename " + src + " to " + dst);
    }
  }

  @Override
  public boolean delete(final Path path, final boolean recursive) throws IOException {
    final String pathStr = toAbsolutePathInString(path);
    try {
      return getMetaClient().remove(pathStr, recursive);
    } catch (TException e) {
      throw new IOException("Failed to delete " + path);
    }
  }

  /**
   * Instantiate and return a new MetaClient for thread-safety
   */
  private SurfMetaService.Client getMetaClient() throws TTransportException {
    return this.metaClientManager.get(this.metaserverAddress);
  }

  /**
   * Instantiate and return a new CacheClientManager for thread-safety
   */
  private CacheClientManager getCacheClientManager() {
    final Configuration conf = this.getConf();
    return new CacheClientManagerImpl(
        conf.getInt(CACHECLIENT_RETRIES_KEY, CACHECLIENT_RETRIES_DEFAULT),
        conf.getInt(CACHECLIENT_RETRIES_INTERVAL_MS_KEY, CACHECLIENT_RETRIES_INTERVAL_MS_DEFAULT),
        conf.getInt(CACHECLIENT_BUFFER_SIZE_KEY, CACHECLIENT_BUFFER_SIZE_DEFAULT));
  }

  private FileStatus toFileStatus(final FileMetaStatus fileMetaStatus) throws URISyntaxException {
    return new FileStatus(
            fileMetaStatus.getLength(),
            fileMetaStatus.isIsdir(),
            fileMetaStatus.getBlock_replication(),
            fileMetaStatus.getBlocksize(),
            fileMetaStatus.getModification_time(),
            fileMetaStatus.getAccess_time(),
            new FsPermission(fileMetaStatus.getPermisison()),
            fileMetaStatus.getOwner(),
            fileMetaStatus.getGroup(),
            null, // TODO: SymLink
            new Path(uri.getScheme(), uri.getAuthority(), fileMetaStatus.getPath()));
  }

  private BlockLocation getBlockLocation(List<NodeInfo> locations, long start, long len) {
    final String[] addresses = new String[locations.size()];
    final String[] hosts = new String[locations.size()];
    final String[] topologyPaths = new String[locations.size()];

    int idx = 0;
    for (final NodeInfo location : locations) {
      addresses[idx] = location.getAddress();
      hosts[idx] = HostAndPort.fromString(location.getAddress()).getHostText();
      topologyPaths[idx] = location.getRack() + "/" + location.getAddress();
      LOG.log(Level.INFO, "BlockLocation: "+addresses[idx]+", "+hosts[idx]+", "+topologyPaths[idx]);
      idx++;
    }

    return new BlockLocation(addresses, hosts, topologyPaths, start, len);
  }

  /**
   * Get string value of absolute path from {@code Path}.
   * For example, {@code toAbsolutePathInString(dir1/fileA)} will return {@code /WORKING_DIR/dir1/fileA}.
   * @param path Relative/Absolute path of a file.
   * @return Path component of the absolute path; URI scheme and authority are dropped out.
   */
  private String toAbsolutePathInString(final Path path) {
    final Path absPath = path.isUriPathAbsolute() ? path : new Path(getWorkingDirectory(), path.toUri().getPath());
    return absPath.toUri().getPath();
  }

  /**
   * Get full URI of {@code path}.
   * @param path Relative/Absolute path of a file.
   * @return Absolute path including URI scheme and authority.
   */
  public Path toAbsoluteSurfPath(final Path path) {
    final String absPathStr = toAbsolutePathInString(path);
    return new Path(uri.getScheme(), uri.getAuthority(), absPathStr);
  }

  public Path toAbsoluteBasePath(final Path surfPath) {
    final URI surfPathUri = surfPath.toUri();
    if (surfPathUri.isAbsolute()) {
      return new Path(baseFsUri.getScheme(), baseFsUri.getAuthority(), surfPathUri.getPath());
    } else {
      return surfPath;
    }
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
}
