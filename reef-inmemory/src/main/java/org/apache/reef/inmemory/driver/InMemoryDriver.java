package org.apache.reef.inmemory.driver;

import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.RunningTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.driver.task.TaskMessage;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;
import org.apache.reef.inmemory.task.InMemoryTask;
import org.apache.reef.inmemory.common.CacheStatusMessage;
import org.apache.reef.inmemory.driver.service.MetaServerParameters;
import org.apache.reef.inmemory.driver.service.SurfMetaServer;

import javax.inject.Inject;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * The driver for InMemory Application
 *  - Point of contact for client
 *  - Contains metadata indicating where cached files are stored
 *  - Manages Tasks
 */
@Unit
public final class InMemoryDriver {
  private static final Logger LOG = Logger.getLogger(InMemoryDriver.class.getName());
  private static final ObjectSerializableCodec<CacheStatusMessage> CODEC = new ObjectSerializableCodec<>();

  private final SurfMetaServer metaService;
  private final CacheManager cacheManager;
  private final int initCacheServers;

  private ExecutorService executor;

  /**
   * Job driver. Instantiated by TANG.
   */
  @Inject
  public InMemoryDriver(final SurfMetaServer metaService,
                        final CacheManager cacheManager,
                        final @Parameter(MetaServerParameters.InitCacheServers.class) int initCacheServers) {
    this.metaService = metaService;
    this.cacheManager = cacheManager;
    this.initCacheServers = initCacheServers;
  }

  /**
   * Handler of StartTime event: Request Evaluators
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);

      cacheManager.requestEvaluator(initCacheServers);

      executor = Executors.newSingleThreadExecutor();
      try {
        executor.execute(metaService);
      } catch (Exception ex) {
        final String message = "Failed to start Surf Meta Service";
        LOG.log(Level.SEVERE, message);
        throw new RuntimeException(message, ex);
      }
    }
  }

  /**
   * Handler of AllocatedEvaluator event: Submit an Task to the allocated evaluator
   */
  public final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting Task to AllocatedEvaluator: {0}", allocatedEvaluator);
      cacheManager.submitContextAndTask(allocatedEvaluator);
    }
  }

  /**
   * Handler of RunningTask event.
   */
  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(RunningTask task) {
      LOG.log(Level.INFO, "Task {0} Running", task.getId());
      cacheManager.addRunningTask(task);
    }
  }

  /**
   * Handler of CompletedTask event.
   */
  public final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(CompletedTask task) {
      LOG.log(Level.INFO, "Task {0} Completed", task.getId());
      cacheManager.removeRunningTask(task.getId());
    }
  }

  /**
   * Handler of StopTime event: Shutting down.
   * TODO Resources has to be released properly.
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      synchronized (executor) {
        if (!executor.isShutdown()) {
          LOG.log(Level.INFO, "Shutdown SurfMetaService now!!");
          executor.shutdown();

          try {
            if (!executor.awaitTermination(10, TimeUnit.SECONDS)) {
              LOG.log(Level.WARNING, "Shutdown SurfMetaService Now!! Data will be lost.");

              executor.shutdownNow();
              executor.awaitTermination(10, TimeUnit.SECONDS);
            }
          } catch (InterruptedException e) {
            executor.shutdownNow();
            Thread.currentThread().interrupt();
          }
        }

        LOG.log(Level.INFO, "DriverStopTime: {0}", stopTime);
      }
    }
  }

  // TODO: extract to a top-level class?
  /**
   * Handler of TaskMessage event: Receive a message from Task
   * TODO Distinguish the type of messages by ID
   */
  public class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(TaskMessage msg) {
      LOG.log(Level.FINE, "TaskMessage: from {0}: {1}",
          new Object[]{msg.getId(), CODEC.decode(msg.get())});

      cacheManager.handleUpdate(msg.getId(), CODEC.decode(msg.get()));
    }
  }
}