package org.apache.reef.inmemory;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.context.ActiveContext;
import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.CompletedTask;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Parameter;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;
import com.microsoft.wake.time.event.StopTime;
import org.apache.reef.inmemory.org.apache.reef.inmemory.worker.CacheTask;

/**
 * The driver class for InMemory Application
 */
@Unit
public final class InMemoryDriver {
  private static final Logger LOG = Logger.getLogger(InMemoryDriver.class.getName());

  private final EvaluatorRequestor requestor;
  private int numRuns = 0;
  private int maxRuns = 1;
  /**
   * Job Driver. Instantiated by TANG.
   */
  @Inject
  public InMemoryDriver(final EvaluatorRequestor requestor,
      final @Parameter(Launch.NumRuns.class) Integer numRuns) {
    this.requestor = requestor;
    this.maxRuns = numRuns;
  }

  /**
   * Get a Task Configuration
   */
  final Configuration getTaskConfiguration() throws BindException {
    return  TaskConfiguration.CONF
        .set(TaskConfiguration.IDENTIFIER, "InMemoryTask")
        .set(TaskConfiguration.TASK, InMemoryTask.class)
//        .set(TaskConfiguration.TASK, CacheTask.class)
        .build();
  }
  
  /**
   * Handler of StartTime event: Request as a single Evaluator
   */
  final class StartHandler implements EventHandler<StartTime> {
    @Override
    public void onNext(final StartTime startTime) {
      LOG.log(Level.INFO, "StartTime: {0}", startTime);
      InMemoryDriver.this.numRuns = 0;
      InMemoryDriver.this.requestor.submit(EvaluatorRequest.newBuilder()
          .setNumber(1)
          .setMemory(128)
          .build());
    }
  }
  
  /**
   * Handler of AllocatedEvaluator event: Submit an Task to the allocated evaluator
   */
  final class EvaluatorAllocatedHandler implements EventHandler<AllocatedEvaluator>{
    @Override
    public void onNext(final AllocatedEvaluator allocatedEvaluator) {
      LOG.log(Level.INFO, "Submitting Task to AllocatedEvaluator: {0}", allocatedEvaluator);
      try{
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
   * Handler of CompletedTask event: Submit another Task or Terminate.
   */
  final class CompletedTaskHandler implements EventHandler<CompletedTask>{
    @Override
    public void onNext(CompletedTask task) {
      synchronized (this) {
        ActiveContext context = task.getActiveContext();
        try {
          if(++numRuns < maxRuns) {
            context.submitTask(getTaskConfiguration());
            LOG.log(Level.INFO, "Submit Task{0}", numRuns);
          } else {
            context.close();
            LOG.info("Done!");
          }
        } catch (BindException e) {
          e.printStackTrace();
          context.close();
          LOG.info("ERROR");
        }
      }
    }
  }
  
  /**
   * Handler of StartTime event: Request as a single Evaluator
   */
  final class StopHandler implements EventHandler<StopTime> {
    @Override
    public void onNext(final StopTime stopTime) {
      synchronized (this) {
        LOG.log(Level.FINEST, "DriverStopTime: {0}", stopTime);
      }
    }
  }

}