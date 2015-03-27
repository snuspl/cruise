package edu.snu.reef.flexion.core;

import com.microsoft.reef.io.network.nggroup.api.driver.CommunicationGroupDriver;
import com.microsoft.reef.io.network.nggroup.api.driver.GroupCommDriver;
import com.microsoft.reef.io.network.nggroup.impl.config.BroadcastOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.GatherOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ReduceOperatorSpec;
import com.microsoft.reef.io.network.nggroup.impl.config.ScatterOperatorSpec;
import edu.snu.reef.flexion.groupcomm.names.*;
import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.task.CompletedTask;
import org.apache.reef.driver.task.FailedTask;
import org.apache.reef.driver.task.TaskConfiguration;
import org.apache.reef.driver.task.TaskMessage;
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
import org.apache.reef.wake.remote.impl.ObjectSerializableCodec;

import javax.inject.Inject;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Driver code for Flexion applications.
 * This class is appropriate for setting up event handlers as well as configuring
 * Broadcast (and/or Scatter) and Reduce (and/or Gather) operations for Group Communication.
 */
@Unit
public final class FlexionDriver {
    private final static Logger LOG = Logger.getLogger(FlexionDriver.class.getName());

    /**
     * Sub-id for Compute Tasks.
     * This object grants different IDs to each task
     * e.g. ComputeTask-0, ComputeTask-1, and so on.
     */
    private final AtomicInteger taskId = new AtomicInteger(0);

    /**
     * ID of the Context that goes under Controller Task.
     * This string is used to distinguish the Context that represents the Controller Task
     * from Contexts that go under Compute Tasks.
     */
    private String ctrlTaskContextId;

    /**
     * Driver that manages Group Communication settings
     */
    private final GroupCommDriver groupCommDriver;

    /**
     * Accessor for data loading service
     * Can check whether a evaluator is configured with the service or not.
     */
    private final DataLoadingService dataLoadingService;

    private final UserJobInfo userJobInfo;

    private final List<UserTaskInfo> userTaskInfoList;
    private final List<CommunicationGroupDriver> commGroupDriverList;
    private final Map<String, Integer> contextToSequence;

    private final ObjectSerializableCodec<Long> codecLong = new ObjectSerializableCodec<>();


    /**
     * This class is instantiated by TANG
     *
     * Constructor for Driver of k-means job.
     * Store various objects as well as configuring Group Communication with
     * Broadcast and Reduce operations to use.
     *
     * @param groupCommDriver manager for Group Communication configurations
     * @param dataLoadingService manager for Data Loading configurations
     */
    @Inject
    private FlexionDriver(final GroupCommDriver groupCommDriver,
                          final DataLoadingService dataLoadingService,
                          final UserJobInfo userJobInfo) throws IllegalAccessException, InstantiationException, NoSuchMethodException, InvocationTargetException {
        this.groupCommDriver = groupCommDriver;
        this.dataLoadingService = dataLoadingService;
        this.userJobInfo = userJobInfo;
        this.userTaskInfoList = userJobInfo.getTaskInfoList();
        this.commGroupDriverList = new LinkedList<>();
        this.contextToSequence = new HashMap<>();
        initializeCommDriver();

    }

    private void initializeCommDriver(){

        int sequence = 0;

        for(UserTaskInfo userTaskInfo : userTaskInfoList) {

            CommunicationGroupDriver commGroup = groupCommDriver.newCommunicationGroup(
                    userTaskInfo.getCommGroupName(),
                    dataLoadingService.getNumberOfPartitions() + 1);

            commGroup.addBroadcast(CtrlMsgBroadcast.class,
                    BroadcastOperatorSpec.newBuilder()
                            .setSenderId(ControllerTask.TASK_ID+"-"+sequence)
                            .setDataCodecClass(SerializableCodec.class)
                            .build());

            if (userTaskInfo.isBroadcastUsed()) {
                commGroup.addBroadcast(DataBroadcast.class,
                        BroadcastOperatorSpec.newBuilder()
                                .setSenderId(ControllerTask.TASK_ID+"-"+sequence)
                                .setDataCodecClass(userTaskInfo.getBroadcastCodecClass())
                                .build());
            }

            if (userTaskInfo.isScatterUsed()) {
                commGroup.addScatter(DataScatter.class,
                        ScatterOperatorSpec.newBuilder()
                                .setSenderId(ControllerTask.TASK_ID+"-"+sequence)
                                .setDataCodecClass(userTaskInfo.getScatterCodecClass())
                                .build());
            }

            if (userTaskInfo.isReduceUsed()) {
                commGroup.addReduce(DataReduce.class,
                        ReduceOperatorSpec.newBuilder()
                                .setReceiverId(ControllerTask.TASK_ID+"-"+sequence)
                                .setDataCodecClass(userTaskInfo.getReduceCodecClass())
                                .setReduceFunctionClass(userTaskInfo.getReduceFunctionClass())
                                .build());
            }

            if (userTaskInfo.isGatherUsed()) {
                commGroup.addGather(DataGather.class,
                        GatherOperatorSpec.newBuilder()
                                .setReceiverId(ControllerTask.TASK_ID+"-"+sequence)
                                .setDataCodecClass(userTaskInfo.getGatherCodecClass())
                                .build());
            }

            commGroupDriverList.add(commGroup);

            commGroup.finalise();

            sequence++;
        }

    }

    final class ActiveContextHandler implements EventHandler<ActiveContext> {
        @Override
        public void onNext(final ActiveContext activeContext) {

            // Case 1: Evaluator configured with a Data Loading context has been given
            // We need to add a Group Communication context above this context.
            //
            // It would be better if the two services could go into the same context, but
            // the Data Loading API is currently constructed to add its own context before
            // allowing any other ones.
            if (!groupCommDriver.isConfigured(activeContext)) {
                Configuration groupCommContextConf = groupCommDriver.getContextConfiguration();
                Configuration groupCommServiceConf = groupCommDriver.getServiceConfiguration();
                Configuration finalServiceConf;

                if (dataLoadingService.isComputeContext(activeContext)) {
                    LOG.log(Level.INFO, "Submitting GroupCommContext for ControllerTask to underlying context");
                    ctrlTaskContextId = getContextId(groupCommContextConf);

                    // Add a Key-Value Store service with the Group Communication service
                    final Configuration keyValueStoreConf = KeyValueStoreService.getServiceConfiguration();
                    finalServiceConf = Configurations.merge(groupCommServiceConf, keyValueStoreConf);

                } else {
                    LOG.log(Level.INFO, "Submitting GroupCommContext for ComputeTask to underlying context");

                    // Add a Data Parse service and a Key-Value Store service with the Group Communication service
                    final Configuration dataParseConf = DataParseService.getServiceConfiguration(userJobInfo.getDataParser());
                    final Configuration keyValueStoreConf = KeyValueStoreService.getServiceConfiguration();
                    finalServiceConf = Configurations.merge(groupCommServiceConf, dataParseConf, keyValueStoreConf);
                }

                activeContext.submitContextAndService(groupCommContextConf, finalServiceConf);

            } else {
                final Configuration partialTaskConf;

                int sequence = 0;
                contextToSequence.put(activeContext.getId(), sequence);
                UserTaskInfo taskInfo = userTaskInfoList.get(sequence);
                CommunicationGroupDriver commGroup = commGroupDriverList.get(sequence);

                // Case 2: Evaluator configured with a Group Communication context has been given,
                //         representing a Controller Task
                // We can now place a Controller Task on top of the contexts.
                if (activeContext.getId().equals(ctrlTaskContextId)) {
                    LOG.log(Level.INFO, "Submit ControllerTask");
                    partialTaskConf = Configurations.merge(
                            TaskConfiguration.CONF
                                    .set(TaskConfiguration.IDENTIFIER, ControllerTask.TASK_ID+"-"+sequence)
                                    .set(TaskConfiguration.TASK, ControllerTask.class)
                                    .build(),
                            Tang.Factory.getTang().newConfigurationBuilder()
                                    .bindImplementation(UserControllerTask.class, taskInfo.getUserCtrlTaskClass())
                                    .bindNamedParameter(CommunicationGroup.class, taskInfo.getCommGroupName().getName())
                                    .build());

                // Case 3: Evaluator configured with a Group Communication context has been given,
                //         representing a Compute Task
                // We can now place a Compute Task on top of the contexts.
                } else {
                    LOG.log(Level.INFO, "Submit ComputeTask");
                    partialTaskConf = Configurations.merge(
                            TaskConfiguration.CONF
                                    .set(TaskConfiguration.IDENTIFIER, ComputeTask.TASK_ID + "-" + taskId.getAndIncrement())
                                    .set(TaskConfiguration.TASK, ComputeTask.class)
                                    .set(TaskConfiguration.ON_SEND_MESSAGE, ComputeTask.class)
                                    .build(),
                            Tang.Factory.getTang().newConfigurationBuilder()
                                    .bindImplementation(UserComputeTask.class, taskInfo.getUserCmpTaskClass())
                                    .bindNamedParameter(CommunicationGroup.class, taskInfo.getCommGroupName().getName())
                                    .build());
                }

                // add the Task to our communication group
                commGroup.addTask(partialTaskConf);
                final Configuration finalTaskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
                activeContext.submitTask(finalTaskConf);
            }
        }
    }

    /**
     * Return the ID of the given Context
     */
    private String getContextId(final Configuration contextConf) {
        try {
            final Injector injector = Tang.Factory.getTang().newInjector(contextConf);
            return injector.getNamedInstance(ContextIdentifier.class);
        } catch (final InjectionException e) {
            throw new RuntimeException("Unable to inject context identifier from context conf", e);
        }
    }

    final class TaskMessageHandler implements EventHandler<TaskMessage> {
        @Override
        public void onNext(final TaskMessage message) {
            final long result = codecLong.decode(message.get());
            final String msg = "Task message " + message.getId() + ": " + result;

            //TODO: use metric to run optimization plan
            LOG.info(msg);
        }
    }

    final class TaskCompletedHandler implements EventHandler<CompletedTask> {

        @Override
        public void onNext(CompletedTask completedTask) {
            LOG.info(completedTask.getId() + " has completed.");

            String contextId = completedTask.getActiveContext().getId();
            int nextSequence = contextToSequence.get(contextId)+1;
            if (nextSequence >= userTaskInfoList.size()) {
                completedTask.getActiveContext().close();
                return;
            }

            contextToSequence.put(contextId, nextSequence);
            UserTaskInfo taskInfo = userTaskInfoList.get(nextSequence);
            CommunicationGroupDriver commGroup = commGroupDriverList.get(nextSequence);
            final Configuration partialTaskConf;

            if (contextId.equals(ctrlTaskContextId)) {
                LOG.log(Level.INFO, "Submit ControllerTask");
                partialTaskConf = Configurations.merge(
                        TaskConfiguration.CONF
                                .set(TaskConfiguration.IDENTIFIER, ControllerTask.TASK_ID+"-"+nextSequence)
                                .set(TaskConfiguration.TASK, ControllerTask.class)
                                .build(),
                        Tang.Factory.getTang().newConfigurationBuilder()
                                .bindImplementation(UserControllerTask.class, taskInfo.getUserCtrlTaskClass())
                                .bindNamedParameter(CommunicationGroup.class, taskInfo.getCommGroupName().getName())
                                .build());
            } else {
                LOG.log(Level.INFO, "Submit ComputeTask");
                partialTaskConf = Configurations.merge(
                        TaskConfiguration.CONF
                                .set(TaskConfiguration.IDENTIFIER, ComputeTask.TASK_ID + "-" + taskId.getAndIncrement())
                                .set(TaskConfiguration.TASK, ComputeTask.class)
                                .set(TaskConfiguration.ON_SEND_MESSAGE, ComputeTask.class)
                                .build(),
                        Tang.Factory.getTang().newConfigurationBuilder()
                                .bindImplementation(UserComputeTask.class, taskInfo.getUserCmpTaskClass())
                                .bindNamedParameter(CommunicationGroup.class, taskInfo.getCommGroupName().getName())
                                .build());
            }
            commGroup.addTask(partialTaskConf);
            final Configuration finalTaskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
            completedTask.getActiveContext().submitTask(finalTaskConf);

        }
    }

    /**
     * When a certain Compute Task fails, we add the Task back and let it participate in
     * Group Communication again. However if the failed Task is the Controller Task,
     * we just shut down the whole job because it's hard to recover the cluster centroid info.
     */
    final class FailedTaskHandler implements EventHandler<FailedTask> {
        @Override
        public void onNext(FailedTask failedTask) {
            LOG.info(failedTask.getId() + " has failed.");

            // Stop the whole job if the failed Task is the Compute Task
            if (failedTask.getActiveContext().get().getId().equals(ctrlTaskContextId)) {
                throw new RuntimeException("Controller Task failed; aborting job");
            }

            String contextId = failedTask.getActiveContext().get().getId();
            int sequence = contextToSequence.get(contextId);
            UserTaskInfo taskInfo = userTaskInfoList.get(sequence);
            CommunicationGroupDriver commGroup = commGroupDriverList.get(sequence);

            final Configuration partialTaskConf = Configurations.merge(
                    TaskConfiguration.CONF
                            .set(TaskConfiguration.IDENTIFIER, ComputeTask.TASK_ID + "-" + taskId.getAndIncrement())
                            .set(TaskConfiguration.TASK, ComputeTask.class)
                            .set(TaskConfiguration.ON_SEND_MESSAGE, ComputeTask.class)
                            .build(),
                    Tang.Factory.getTang().newConfigurationBuilder()
                            .bindImplementation(UserComputeTask.class, taskInfo.getUserCmpTaskClass())
                            .bindNamedParameter(CommunicationGroup.class, taskInfo.getCommGroupName().getName())
                            .build());

            // Re-add the failed Compute Task
            commGroup.addTask(partialTaskConf);

            final Configuration taskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);

            failedTask.getActiveContext().get().submitTask(taskConf);

        }
    }




}
