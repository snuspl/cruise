package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.nggroup.api.driver.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.driver.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import edu.snu.reef.flexion.groupcomm.names.CommunicationGroup;
import edu.snu.reef.flexion.groupcomm.names.CtrlMsgBroadcast;
import edu.snu.reef.flexion.groupcomm.names.DataBroadcast;
import edu.snu.reef.flexion.groupcomm.names.DataReduce;
import edu.snu.reef.flexion.groupcomm.subs.DataReduceFunction;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.EvaluatorRequestor;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.evaluator.context.parameters.ContextIdentifier;
import org.apache.reef.io.data.loading.api.DataLoadingService;
import org.apache.reef.io.serialization.SerializableCodec;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Unit;
import org.apache.reef.tang.exceptions.InjectionException;
import org.apache.reef.wake.EventHandler;

import javax.inject.Inject;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

@Unit
public class FlexionDriver {
  private final static Logger LOG = Logger.getLogger(FlexionDriver.class.getName());

  private final EvaluatorRequestor requestor;
  private final GroupCommDriver groupCommDriver;
  private final DataLoadingService dataLoadingService;
  private final CommunicationGroupDriver commGroup;

  private final AtomicInteger cmpTaskId = new AtomicInteger(0);
  private String ctrlTaskContextId;

  @Inject
  private FlexionDriver(final EvaluatorRequestor requestor,
                        final GroupCommDriver groupCommDriver,
                        final DataLoadingService dataLoadingService) {
    this.requestor = requestor;
    this.groupCommDriver = groupCommDriver;
    this.dataLoadingService = dataLoadingService;

    this.commGroup = groupCommDriver
        .newCommunicationGroup(CommunicationGroup.class,
            dataLoadingService.getNumberOfPartitions() + 1);

    this.commGroup
        .addBroadcast(CtrlMsgBroadcast.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(ControllerTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addBroadcast(DataBroadcast.class,
            BroadcastOperatorSpec.newBuilder()
                .setSenderId(ControllerTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .build())
        .addReduce(DataReduce.class,
            ReduceOperatorSpec.newBuilder()
                .setReceiverId(ControllerTask.TASK_ID)
                .setDataCodecClass(SerializableCodec.class)
                .setReduceFunctionClass(DataReduceFunction.class)
                .build())
        .finalise();
  }

  final class ActiveContextHandler implements EventHandler<ActiveContext> {
    @Override
    public void onNext(final ActiveContext activeContext) {
      if (!groupCommDriver.isConfigured(activeContext)) {
        Configuration groupCommContextConf = groupCommDriver.getContextConfiguration();
        Configuration groupCommServiceConf = groupCommDriver.getServiceConfiguration();
        Configuration finalServiceConf;

        if (dataLoadingService.isComputeContext(activeContext)) {
          LOG.log(Level.INFO, "Submitting GroupCommContext for ControllerTask to underlying context");
          ctrlTaskContextId = getContextId(groupCommContextConf);
          finalServiceConf = groupCommServiceConf;

        } else {
          LOG.log(Level.INFO, "Submitting GroupCommContext for ComputeTask to underlying context");
          finalServiceConf = groupCommServiceConf;
        }

        activeContext.submitContextAndService(groupCommContextConf, finalServiceConf);

      } else {
        final Configuration partialTaskConf;

        // Case 2: Evaluator configured with a Group Communication context has been given,
        //         representing a Controller Task
        // We can now place a Controller Task on top of the contexts.
        if (activeContext.getId().equals(ctrlTaskContextId)) {
          LOG.log(Level.INFO, "Submit ControllerTask");
          partialTaskConf = Configurations.merge(
              TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, ControllerTask.TASK_ID)
                  .set(TaskConfiguration.TASK, ControllerTask.class)
                  .build());

          // Case 3: Evaluator configured with a Group Communication context has been given,
          //         representing a Compute Task
          // We can now place a Compute Task on top of the contexts.
        } else {
          LOG.log(Level.INFO, "Submit ComputeTask");
          partialTaskConf = Configurations.merge(
              TaskConfiguration.CONF
                  .set(TaskConfiguration.IDENTIFIER, ComputeTask.TASK_ID + "-" + cmpTaskId.getAndIncrement())
                  .set(TaskConfiguration.TASK, ComputeTask.class)
                  .build());
        }

        // add the Task to our communication group
        commGroup.addTask(partialTaskConf);
        final Configuration finalTaskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
        activeContext.submitTask(finalTaskConf);
      }
    }
  }

  private String getContextId(final Configuration contextConf) {
    try {
      final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
      return injector.getNamedInstance(ContextIdentifier.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Unable to inject context identifier from context conf", e);
    }
  }
}
