package org.apache.reef.inmemory.client;

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

import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
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
 * In the current design, SurfFS is a read-only filesystem. Thus,
 *   create, append, rename, delete, mkdirs
 * throw an UnsupportedOperationException
 */
public final class SurfFS extends FileSystem {

  public static final String BASE_FS_ADDRESS_KEY = "surf.basefs";
  public static final String BASE_FS_ADDRESS_DEFAULT = "hdfs://localhost:9000";

  private static final Logger LOG = Logger.getLogger(SurfFS.class.getName());

  private static final int NUM_TRIES = 4;
  private static final int WAIT_TRIES = 500;

  // These cannot be final, because the empty constructor + intialize() are called externally
  private FileSystem baseFs;
  private SurfMetaService.Client driverClient;
  private final Map<String, SurfCacheService.Client> taskClients = new HashMap<>(); // TODO: this assumes single threaded access

  private URI uri;
  private URI baseFsUri;

  public SurfFS() {
  }

  protected SurfFS(final FileSystem baseFs,
                   final SurfMetaService.Client driverClient) {
    this.baseFs = baseFs;
    this.driverClient = driverClient;
  }

  private static SurfMetaService.Client getDriverClient(String host, int port)
          throws TTransportException {
    TTransport transport = new TFramedTransport(new TSocket(host, port));
    transport.open();
    TProtocol protocol = new TMultiplexedProtocol(
            new TCompactProtocol(transport),
            SurfMetaService.class.getName());
    return new SurfMetaService.Client(protocol);
  }

  // TODO: may not be general (e.g., IPv6 addresses)
  private SurfCacheService.Client getTaskClient(String address)
          throws TTransportException {
    String[] split = address.split(":");
    if (split.length == 2) {
      String host = split[0];
      int port = Integer.parseInt(split[1]);

      TTransport transport = new TFramedTransport(new TSocket(host, port));
      transport.open();
      TProtocol protocol = new TMultiplexedProtocol(
              new TCompactProtocol(transport),
              SurfCacheService.class.getName());
      return new SurfCacheService.Client(protocol);
    } else {
      throw new RuntimeException("Bad address: "+address);
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

  private ByteBuffer getBlockBuffer(BlockInfo block) throws TException {
    String location = block.getLocationsIterator().next();

    if (!taskClients.containsKey(location)) {
      LOG.log(Level.INFO, "Connecting to cache service at: "+location);
      taskClients.put(location, getTaskClient(location));
      LOG.log(Level.INFO, "Connected! to cache service at: "+location);
    }
    SurfCacheService.Client client = taskClients.get(location);

    LOG.log(Level.INFO, "Sending block request: "+block);
    for (int i = 0; i < NUM_TRIES; i++) {
      try {
        return client.getData(block);
      } catch (BlockLoadingException e) {
        if (i < NUM_TRIES-1) {
          LOG.log(Level.FINE, "BlockLoadingException, load started: "+e.getTimeStarted());
          try {
            Thread.sleep(WAIT_TRIES);
          } catch (InterruptedException ie) {
            LOG.log(Level.WARNING, "Sleep interrupted: "+ie);
          }
        }
      } catch (BlockNotFoundException e) {
        if (i < NUM_TRIES-1) {
          LOG.log(Level.FINE, "BlockNotFoundException at "+System.currentTimeMillis());
          try {
            Thread.sleep(WAIT_TRIES);
          } catch (InterruptedException ie) {
            LOG.log(Level.WARNING, "Sleep interrupted: "+ie);
          }
        }
      }
    }
    LOG.log(Level.WARNING, "Exception even after "+NUM_TRIES+" tries. Aborting.");
    throw new BlockNotFoundException();
  }

  @Override
  public synchronized FSDataInputStream open(Path path, final int bufferSize) throws IOException {
    // Lazy loading (for now)
    if (this.driverClient == null) {
      try {
        this.driverClient = getDriverClient("localhost", 18000); // TODO: use conf
      } catch (TTransportException e) {
        throw new RuntimeException(e);
      }
    }

    try {
      LOG.log(Level.INFO, "getFileMeta called on: "+path+", using: "+path.toUri().getPath());
      FileMeta metadata = driverClient.getFileMeta(path.toUri().getPath());
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
