package org.apache.reef.inmemory.driver;

import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.RunningTask;
import org.apache.reef.driver.task.TaskMessage;
import org.apache.reef.inmemory.driver.service.MetaServerParameters;
import org.apache.reef.inmemory.driver.service.SurfMetaServer;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.wake.EStage;
import org.apache.reef.wake.EventHandler;
import org.apache.reef.wake.time.event.StartTime;
import org.apache.reef.wake.time.event.StopTime;

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

  private final SurfMetaServer metaService;
  private final CacheNodeManager cacheNodeManager;
  private final EStage<TaskMessage> taskMessageEStage;
  private final int initCacheServers;

  private ExecutorService executor;

  /**
   * Job driver. Instantiated by TANG.
   */
  @Inject
  public InMemoryDriver(final SurfMetaServer metaService,
                        final CacheNodeManager cacheNodeManager,
                        final EStage<TaskMessage> taskMessageEStage,
                        final @Parameter(MetaServerParameters.InitCacheServers.class) int initCacheServers) {
    this.metaService = metaService;
    this.cacheNodeManager = cacheNodeManager;
    this.taskMessageEStage = taskMessageEStage;
    this.initCacheServers = initCacheServers;
  }

  /**
   * Handler of StartTime event: Request Evaluators
   */
  public final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);

      cacheNodeManager.requestEvaluator(initCacheServers);

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
      cacheNodeManager.submitContextAndTask(allocatedEvaluator);
    }
  }

  /**
   * Handler of RunningTask event.
   */
  public final class RunningTaskHandler implements EventHandler<RunningTask> {
    @Override
    public void onNext(RunningTask task) {
      LOG.log(Level.INFO, "Task {0} Running", task.getId());
      cacheNodeManager.addRunningTask(task);
    }
  }

  /**
   * Handler of CompletedTask event.
   */
  public final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(CompletedTask task) {
      LOG.log(Level.INFO, "Task {0} Completed", task.getId());
      cacheNodeManager.removeRunningTask(task.getId());
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

  /**
   * Delegate handling to the EStage
   */
  public class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(TaskMessage msg) {
      taskMessageEStage.onNext(msg);
    }
  }
}