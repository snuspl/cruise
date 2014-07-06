package org.apache.reef.inmemory.fs.service;

import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.tang.annotations.Parameter;
import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.fs.SurfMetaManager;
import org.apache.reef.inmemory.fs.entity.BlockInfo;
import org.apache.reef.inmemory.fs.entity.FileMeta;
import org.apache.reef.inmemory.fs.entity.User;
import org.apache.reef.inmemory.fs.exceptions.AllocationFailedException;
import org.apache.reef.inmemory.fs.exceptions.FileNotFoundException;
import org.apache.reef.inmemory.fs.exceptions.SubmissionFailedException;
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
 * Implements thrift server operations, for both FileSystem client and cache management CLI.
 * @see org.apache.reef.inmemory.cli.CLI
 */
public final class SurfMetaServer implements SurfMetaService.Iface, SurfManagementService.Iface, Runnable, AutoCloseable {

  private static final Logger LOG = Logger.getLogger(SurfMetaServer.class.getName());

  private final int port;
  private final int timeout;
  private final int numThreads;
  TServer server = null;

  private final SurfMetaManager metaManager;
  private final EvaluatorRequestor evaluatorRequestor;

  @Inject
  public SurfMetaServer(final SurfMetaManager metaManager,
                        final EvaluatorRequestor evaluatorRequestor,
                        final @Parameter(MetaServerParameters.Port.class) int port,
                        final @Parameter(MetaServerParameters.Timeout.class) int timeout,
                        final @Parameter(MetaServerParameters.Threads.class) int numThreads) {
    this.metaManager = metaManager;
    this.evaluatorRequestor = evaluatorRequestor;

    this.port = port;
    this.timeout = timeout;
    this.numThreads = numThreads;
  }

  @Override
  public FileMeta getFileMeta(String path) throws FileNotFoundException, TException {
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
  public boolean load(String path) throws TException {
    LOG.log(Level.INFO, "CLI load command for path {0}", path);
    try {
      List<BlockInfo> blocks = metaManager.getBlocks(new Path(path), new User()).getBlocks();
      for (BlockInfo block : blocks) {
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
  public String addCacheNode(int port, int memory)
          throws AllocationFailedException, SubmissionFailedException, TException {
    LOG.log(Level.INFO, "CLI addCacheNode command with port {0}, memory {1}",
            new Object[]{port, memory});
    evaluatorRequestor.submit(EvaluatorRequest.newBuilder()
            .setNumber(1)
            .setMemory(memory)
            .build());
    // TODO: wait until node is created, and then return
    return "Submitted";
  }

  @Override
  public void run() {
    try {
      TNonblockingServerTransport serverTransport = new TNonblockingServerSocket(this.port, this.timeout);

      TMultiplexedProcessor processor = new TMultiplexedProcessor();
      SurfMetaService.Processor<SurfMetaService.Iface> metaProcessor =
              new SurfMetaService.Processor<SurfMetaService.Iface>(this);
      processor.registerProcessor(SurfMetaService.class.getName(), metaProcessor);
      SurfManagementService.Processor<SurfManagementService.Iface> managementProcessor =
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
