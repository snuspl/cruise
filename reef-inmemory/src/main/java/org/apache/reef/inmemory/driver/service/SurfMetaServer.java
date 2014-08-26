package org.apache.reef.inmemory.driver.service;

import com.microsoft.tang.annotations.Parameter;
import com.microsoft.wake.remote.NetUtils;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.common.exceptions.FileNotFoundException;
import org.apache.reef.inmemory.common.replication.AvroReplicationSerializer;
import org.apache.reef.inmemory.common.replication.Rules;
import org.apache.reef.inmemory.common.service.SurfManagementService;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.reef.inmemory.driver.CacheManager;
import org.apache.reef.inmemory.driver.SurfMetaManager;
import org.apache.reef.inmemory.driver.CacheNode;
import org.apache.reef.inmemory.driver.replication.ReplicationPolicy;
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
  private final CacheManager cacheManager;
  private final ServiceRegistry serviceRegistry;
  private final ReplicationPolicy replicationPolicy;

  @Inject
  public SurfMetaServer(final SurfMetaManager metaManager,
                        final CacheManager cacheManager,
                        final ServiceRegistry serviceRegistry,
                        final ReplicationPolicy replicationPolicy,
                        final @Parameter(MetaServerParameters.Port.class) int port,
                        final @Parameter(MetaServerParameters.Timeout.class) int timeout,
                        final @Parameter(MetaServerParameters.Threads.class) int numThreads) {
    this.metaManager = metaManager;
    this.cacheManager = cacheManager;
    this.serviceRegistry = serviceRegistry;
    this.replicationPolicy = replicationPolicy;

    this.port = port;
    this.timeout = timeout;
    this.numThreads = numThreads;
  }

  @Override
  public FileMeta getFileMeta(final String path) throws FileNotFoundException, TException {
    try {
      return metaManager.getFile(new Path(path), new User());
    } catch (java.io.FileNotFoundException e) {
      throw new FileNotFoundException("File not found at "+path);
    } catch (IOException e) {
      throw new FileNotFoundException(e.getMessage());
    } catch (Throwable e) {
      LOG.log(Level.SEVERE, "Get metadata failed for "+path, e);
      throw new TException(e);
    }
  }

  @Override
  public String getStatus() throws TException {
    LOG.log(Level.INFO, "CLI status command");
    StringBuilder builder = new StringBuilder();
    for (CacheNode cache : cacheManager.getCaches()) {
      builder.append(cache.getAddress())
             .append(" : ")
             .append(cache.getLatestStatistics())
             .append('\n');
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
      final List<BlockInfo> blocks = metaManager.getFile(new Path(path), new User()).getBlocks();
      if (blocks == null) {
        return true;
      }

      for (final BlockInfo block : blocks) {
        LOG.log(Level.INFO, "Loaded block " + block.getBlockId() + " for " + path);
      }
      return true;
    } catch (java.io.FileNotFoundException e) {
      throw new FileNotFoundException("File not found at "+path);
    } catch (Throwable e) {
      LOG.log(Level.SEVERE, "Load failed for "+path, e);
      throw new TException(e);
    }
  }

  @Override
  public String addCacheNode(final int memory) throws TException {
    LOG.log(Level.INFO, "CLI addCacheNode command with memory {0}", memory);
    if (memory == 0) {
      cacheManager.requestEvaluator(1);
    } else {
      cacheManager.requestEvaluator(1, memory);
    }
    return "Submitted";
  }

  @Override
  public String getReplication() throws org.apache.reef.inmemory.common.exceptions.IOException, TException {
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

  public synchronized void handleUpdate(final String taskId, final CacheStatusMessage msg) {
    cacheManager.handleHeartbeat(taskId, msg);
    final CacheNode cache = cacheManager.getCache(taskId);
    if (cache != null) {
      metaManager.applyUpdates(cache, msg.getUpdates());
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
      e.printStackTrace();
    } finally {
      if (this.server != null && this.server.isServing())
        this.server.stop();
    }
  }

  @Override
  public void close() throws Exception {
    if (this.server != null && this.server.isServing())
      this.server.stop();
  }
}
