package org.apache.reef.inmemory.driver.service;

import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.BlockInfo;
import org.apache.reef.inmemory.common.entity.FileMeta;
import org.apache.reef.inmemory.common.entity.User;
import org.apache.reef.inmemory.common.exceptions.FileNotFoundException;
import org.apache.reef.inmemory.common.service.SurfManagementService;
import org.apache.reef.inmemory.common.service.SurfMetaService;
import org.apache.reef.inmemory.driver.CacheManager;
import org.apache.reef.inmemory.driver.SurfMetaManager;
import org.apache.thrift.TException;
import org.apache.thrift.TMultiplexedProcessor;
import org.apache.thrift.server.THsHaServer;
import org.apache.thrift.server.TServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TNonblockingServerTransport;

import javax.inject.Inject;
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

  @Inject
  public SurfMetaServer(final SurfMetaManager metaManager,
                        final CacheManager cacheManager,
                        final @Parameter(MetaServerParameters.Port.class) int port,
                        final @Parameter(MetaServerParameters.Timeout.class) int timeout,
                        final @Parameter(MetaServerParameters.Threads.class) int numThreads) {
    this.metaManager = metaManager;
    this.cacheManager = cacheManager;

    this.port = port;
    this.timeout = timeout;
    this.numThreads = numThreads;
  }

  @Override
  public FileMeta getFileMeta(final String path) throws FileNotFoundException, TException {
    try {
      return metaManager.getBlocks(new Path(path), new User());
    } catch (java.io.FileNotFoundException e) {
      throw new FileNotFoundException("File not found at "+path);
    } catch (Throwable e) {
      LOG.log(Level.SEVERE, "Get metadata failed for "+path, e);
      throw new TException(e);
    }
  }

  @Override
  public long clear() throws TException {
    LOG.log(Level.INFO, "CLI clear command");
    return metaManager.clear();
  }

  // TODO: return loaded Task address and absolute path
  @Override
  public boolean load(final String path) throws TException {
    LOG.log(Level.INFO, "CLI load command for path {0}", path);
    try {
      final List<BlockInfo> blocks = metaManager.getBlocks(new Path(path), new User()).getBlocks();
      for (final BlockInfo block : blocks) {
        LOG.log(Level.INFO, "Loaded block " + block.getBlockId() + " for " + path);
      }
      return true;
    } catch (java.io.FileNotFoundException e) {
      throw new FileNotFoundException("File not found at "+path);
    } catch (Throwable e) {
      LOG.log(Level.SEVERE, "Load failed for "+path, e);
      return false;
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
