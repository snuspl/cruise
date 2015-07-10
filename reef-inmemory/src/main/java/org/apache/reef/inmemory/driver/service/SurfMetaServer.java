package org.apache.reef.inmemory.driver.service;

import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.common.entity.*;
import org.apache.reef.inmemory.common.exceptions.FileNotFoundException;
import org.apache.reef.inmemory.common.replication.Action;
import org.apache.reef.inmemory.common.replication.AvroReplicationSerializer;
import org.apache.reef.inmemory.common.replication.Rules;
import org.apache.reef.inmemory.common.service.SurfManagementService;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.CacheNodeManager;
import org.apache.reef.inmemory.driver.SurfMetaManager;
import org.apache.reef.inmemory.driver.locality.LocationSorter;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
import org.apache.reef.inmemory.driver.write.WritingCacheSelectionPolicy;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.wake.remote.NetUtils;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import javax.inject.Inject;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Implements thrift server operations, for both FileSystem client and task management CLI.
 * @see org.apache.reef.inmemory.client.cli.CLI
 */
public final class SurfMetaServer implements SurfMetaService.Iface, SurfManagementService.Iface, Runnable, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(SurfMetaServer.class.getName());

  private final int port;
  private final int timeout;
  private final int numThreads;
  TServer server = null;

  private final SurfMetaManager metaManager;
  private final CacheNodeManager cacheNodeManager;
  private final ServiceRegistry serviceRegistry;
  private final ReplicationPolicy replicationPolicy;
  private final WritingCacheSelectionPolicy writingCacheSelector;
  private final LocationSorter locationSorter;

  @Inject
  public SurfMetaServer(final SurfMetaManager metaManager,
                        final CacheNodeManager cacheNodeManager,
                        final ServiceRegistry serviceRegistry,
                        final WritingCacheSelectionPolicy writingCacheSelector,
                        final ReplicationPolicy replicationPolicy,
                        final LocationSorter locationSorter,
                        @Parameter(MetaServerParameters.Port.class) final int port,
                        @Parameter(MetaServerParameters.Timeout.class) final int timeout,
                        @Parameter(MetaServerParameters.Threads.class) final int numThreads) {
    this.metaManager = metaManager;
    this.cacheNodeManager = cacheNodeManager;
    this.serviceRegistry = serviceRegistry;
    this.replicationPolicy = replicationPolicy;
    this.writingCacheSelector = writingCacheSelector;
    this.locationSorter = locationSorter;

    this.port = port;
    this.timeout = timeout;
    this.numThreads = numThreads;
  }

  /**
   * Return the fileMeta from MetaTree, loading it from HDFS if not exists
   */
  @Override
  public FileMeta getOrLoadFileMeta(final String path, final String clientHostname) throws FileNotFoundException, TException {
    try {
      final FileMeta fileMeta = metaManager.getOrLoadFileMeta(path);
      return locationSorter.sortMeta(fileMeta, clientHostname);
    } catch (IOException e) {
      throw new FileNotFoundException(e.getMessage());
    }
  }

  @Override
  public boolean exists(final String path) throws TException {
    return metaManager.exists(path);
  }

  @Override
  public List<FileMetaStatus> listFileMetaStatus(final String path) throws FileNotFoundException, TException {
    try {
      return metaManager.listFileMetaStatus(path);
    } catch (IOException e) {
      throw new FileNotFoundException(e.getMessage());
    }
  }

  @Override
  public FileMetaStatus getFileMetaStatus(final String path) throws FileNotFoundException, TException {
    try {
      return metaManager.getFileMetaStatus(path);
    } catch (IOException e) {
      throw new FileNotFoundException(e.getMessage());
    }
  }

  @Override
  public void create(final String path, final long blockSize, final short baseFsReplication) throws TException {
    try {
      metaManager.create(path, blockSize, baseFsReplication);
    } catch (IOException e) {
      throw new TException(e);
    }
  }

  @Override
  public boolean mkdirs(final String path) throws TException {
    try {
      return metaManager.mkdirs(path);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "mkdirs failed at baseFS for " + path, e);
      throw new TException(e);
    }
  }

  @Override
  public boolean rename(final String src, final String dst) throws TException {
    try {
      return metaManager.rename(src, dst);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "rename failed at baseFS for " + src + " to " + dst, e);
      throw new TException(e);
    }
  }

  @Override
  public boolean remove(final String path, final boolean recursive) throws TException {
    try {
      return metaManager.remove(path, recursive);
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "remove failed at baseFS for " + path, e);
      throw new TException(e);
    }
  }

  @Override
  public WriteableBlockMeta allocateBlock(final String path,
                                          final long offset,
                                          final String clientAddress) throws TException {
    try {
      final FileMeta meta = metaManager.getFileMeta(path);
      final Action action = replicationPolicy.getReplicationAction(path, meta);
      final int replication = action.getReplication(); // TODO: might be better to be of type 'short' in avro
      final List<NodeInfo> selected = writingCacheSelector.select(cacheNodeManager.getCaches(), replication);
      final boolean pin = action.getPin();
      final BlockMeta blockMeta = new BlockMeta(meta.getFileId(), offset, meta.getBlockSize(), selected);
      return new WriteableBlockMeta(blockMeta, pin, (short) replication);
    } catch (IOException e) {
      throw new FileNotFoundException(e.getMessage());
    }
  }

  @Override
  public boolean completeFile(final String path, final long fileSize) throws TException {
    try {
      final FileMeta meta = metaManager.getFileMeta(path);
      LOG.log(Level.INFO, "Compare the file size of {0} : Expected {1} / Actual {2}",
          new Object[] {path, fileSize, meta.getFileSize()});
      return fileSize == meta.getFileSize();
    } catch (IOException e) {
      throw new FileNotFoundException(e.getMessage());
    }
  }

  //////////////////////////////////////
  // Methods from SurfManagementService

  @Override
  public String getStatus() throws TException {
    LOG.log(Level.INFO, "CLI status command");
    final StringBuilder builder = new StringBuilder();
    final long currentTimestamp = System.currentTimeMillis();
    final List<CacheNode> caches = cacheNodeManager.getCaches();
    builder.append("Number of caches: " + caches.size() + "\n");
    for (CacheNode cache : caches) {
      appendBasicStatus(builder, cache, currentTimestamp);
      appendStopCause(builder, cache);
      builder.append('\n');
    }
    return builder.toString();
  }

  @Override
  public long clear() throws TException {
    LOG.log(Level.INFO, "CLI clear command");
    return metaManager.clear();
  }

  @Override
  public boolean load(final String path) throws TException {
    LOG.log(Level.INFO, "CLI load command for path {0}", path);
    try {
      metaManager.getOrLoadFileMeta(path);
      LOG.log(Level.INFO, "Load succeeded for "+path);
      return true;
    } catch (IOException e) {
      LOG.log(Level.SEVERE, "Load failed for "+path, e);
      throw new TException(e);
    }
  }

  @Override
  public String addCacheNode(final int memory) throws TException {
    LOG.log(Level.INFO, "CLI addCacheNode command with memory {0}", memory);
    if (memory == 0) {
      cacheNodeManager.requestEvaluator(1);
    } else {
      cacheNodeManager.requestEvaluator(1, memory);
    }
    return "Submitted";
  }

  @Override
  public String getReplication() throws TException {
    LOG.log(Level.INFO, "CLI replicationList command");
    final Rules rules = replicationPolicy.getRules();
    if (rules == null) {
      return "null";
    } else {
      try {
        final ByteArrayOutputStream out = new ByteArrayOutputStream();
        AvroReplicationSerializer.toStream(rules, out);
        return out.toString();
      } catch (IOException e) {
        throw new org.apache.reef.inmemory.common.exceptions.IOException(e.getMessage());
      }
    }
  }

  @Override
  public boolean setReplication(String rulesString) throws org.apache.reef.inmemory.common.exceptions.IOException {
    try {
      final Rules rules = AvroReplicationSerializer.fromString(rulesString);
      replicationPolicy.setRules(rules);
      return true;
    } catch (IOException e) {
      throw new org.apache.reef.inmemory.common.exceptions.IOException(e.getMessage());
    }
  }

  /**
   * Handle the update/heartbeat message from the cache Task, by passing to
   * the CacheManager and SurfMetaManager.
   */
  public synchronized void handleUpdate(final String taskId, final CacheStatusMessage msg) {
    cacheNodeManager.handleHeartbeat(taskId, msg);
    final CacheNode cache = cacheNodeManager.getCache(taskId);
    if (cache != null) {
      metaManager.applyCacheNodeUpdates(cache, msg.getUpdates());
    }
  }

  @Override
  public void run() {
    try {
      final TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(this.port, this.timeout);

      final TMultiplexedProcessor processor = new TMultiplexedProcessor();
      final SurfMetaService.Processor<SurfMetaService.Iface> metaProcessor =
          new SurfMetaService.Processor<SurfMetaService.Iface>(this);
      processor.registerProcessor(SurfMetaService.class.getName(), metaProcessor);
      final SurfManagementService.Processor<SurfManagementService.Iface> managementProcessor =
          new SurfManagementService.Processor<SurfManagementService.Iface>(this);
      processor.registerProcessor(SurfManagementService.class.getName(), managementProcessor);

      this.server = new THsHaServer(
        new org.apache.thrift.server.THsHaServer.Args(serverTransport).processor(processor)
          .protocolFactory(new org.apache.thrift.protocol.TCompactProtocol.Factory())
          .workerThreads(this.numThreads));

      // Register just before serving
      serviceRegistry.register(NetUtils.getLocalAddress(), port);

      this.server.serve();
    } catch (Exception e) {
      LOG.log(Level.SEVERE, "Exception occurred while running MetaServer", e);
    } finally {
      if (this.server != null && this.server.isServing()) {
        this.server.stop();
      }
    }
  }

  @Override
  public void close() throws Exception {
    if (this.server != null && this.server.isServing()) {
      this.server.stop();
    }
  }

  private void appendBasicStatus(final StringBuilder builder,
                                final CacheNode cache,
                                final long currentTimestamp) {
    builder.append(cache.getAddress())
        .append(" : ")
        .append(cache.getLatestStatistics())
        .append(" : ")
        .append(currentTimestamp - cache.getLatestTimestamp())
        .append(" ms ago");
  }

  private void appendStopCause(final StringBuilder builder,
                               final CacheNode cache) {
    if (cache.getStopCause() != null) {
      builder.append(" : ")
          .append(cache.getStopCause());
    }
  }
}
