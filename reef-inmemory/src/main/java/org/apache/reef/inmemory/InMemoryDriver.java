package org.apache.reef.inmemory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import org.apache.reef.inmemory.fs.service.SurfMetaServiceImpl;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.reef.driver.task.TaskMessage;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.remote.impl.ObjectSerializableCodec;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;

/**
 * The driver class for InMemory Application
 */
@Unit
public final class InMemoryDriver {
  private static final Logger LOG = Logger.getLogger(InMemoryDriver.class.getName());
  private static final ObjectSerializableCodec<String> CODEC = new ObjectSerializableCodec<>();

  private final EvaluatorRequestor requestor;
  private ExecutorService executor;

  /**
   * Job Driver. Instantiated by TANG.
   */
  @Inject
  public InMemoryDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
  }

  /**
   * Get a Task Configuration
   */
  final Configuration getTaskConfiguration() throws BindException {
    return TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, "InMemoryTask")
        .set(TaskConfiguration.TASK, InMemoryTask.class)
        .set(TaskConfiguration.ON_SEND_MESSAGE, InMemoryTask.class)
        .build();
  }

  /**
   * Handler of StartTime event: Request as a single Evaluator
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);
      InMemoryDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(128)
          .build());

      ExecutorService executor = Executors.newSingleThreadExecutor();
      executor.execute(new SurfMetaServiceImpl());
    }
  }

  /**
   * Handler of AllocatedEvaluator event: Submit an Task to the allocated evaluator
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator> {
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting Task to AllocatedEvaluator: {0}", allocatedEvaluator);
      try {
        final Configuration contextConf = ContextConfiguration.CONF
            .set(ContextConfiguration.IDENTIFIER, "InMemoryContext")
            .build();
        final Configuration taskConf = getTaskConfiguration();
        allocatedEvaluator.submitContextAndTask(contextConf, taskConf);
      } catch (final BindException ex) {
        final String message = "Failed to bind Task.";
        LOG.log(Level.SEVERE, message);
        throw new RuntimeException(message, ex);
      }
    }
  }

  /**
   * Handler of CompletedTask event.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask> {
    @Override
    public void onNext(CompletedTask task) {
      LOG.log(Level.INFO, "Task {0} Completed", task.getId());
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
   * Handler of TaskMessage event: Receive a message from Task
   * TODO Distinguish the type of messages by ID
   */
  public class TaskMessageHandler implements EventHandler<TaskMessage> {
    @Override
    public void onNext(TaskMessage msg) {
      LOG.log(Level.INFO, "TaskMessage: from {0}: {1}",
          new Object[]{msg.getId(), CODEC.decode(msg.get())});
    }
  }
}