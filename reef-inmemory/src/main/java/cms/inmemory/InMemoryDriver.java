package cms.inmemory;

import java.util.logging.Level;
import java.util.logging.Logger;

import javax.inject.Inject;

import com.microsoft.reef.driver.context.ContextConfiguration;
import com.microsoft.reef.driver.evaluator.AllocatedEvaluator;
import com.microsoft.reef.driver.evaluator.EvaluatorRequest;
import com.microsoft.reef.driver.evaluator.EvaluatorRequestor;
import com.microsoft.reef.driver.task.TaskConfiguration;
import com.microsoft.tang.Configuration;
import com.microsoft.tang.annotations.Unit;
import com.microsoft.tang.exceptions.BindException;
import com.microsoft.wake.EventHandler;
import com.microsoft.wake.time.event.StartTime;

/**
 * The driver class for InMemory Application
 */
@Unit
public final class InMemoryDriver {
  private static final Logger LOG = Logger.getLogger(InMemoryDriver.class.getName());

  private final EvaluatorRequestor requestor;

  /**
   * Job Driver. Instantiated by TANG.
   */
  @Inject
  public InMemoryDriver(final EvaluatorRequestor requestor) {
    this.requestor = requestor;
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
          .setMemory(64)
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
        
        final Configuration taskConf = TaskConfiguration.CONF
            .set(TaskConfiguration.IDENTIFIER, "InMemoryTask")
            .set(TaskConfiguration.TASK, InMemoryTask.class)
            .build();

         allocatedEvaluator.submitContextAndTask(contextConf, taskConf);
      } catch (final BindException ex) {
        final String message = "Failed to bind Task.";
        LOG.log(Level.SEVERE, message);
        throw new RuntimeException(message, ex);
      }
    }
  }
}