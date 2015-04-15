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

    /**
     * Job to execute
     */
    private final UserJobInfo userJobInfo;

    /**
     * List of stages composing the job to execute
     */
    private final List<StageInfo> stageInfoList;

    /**
     * List of communication group drivers.
     * Each group driver is matched to the corresponding stage.
     */
    private final List<CommunicationGroupDriver> commGroupDriverList;

    /**
     * Map to record which stage is being executed by each evaluator which is identified by context id
     */
    private final Map<String, Integer> contextToStageSequence;

    private final ObjectSerializableCodec<Long> codecLong = new ObjectSerializableCodec<>();

    private final UserParameters userParameters;

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
                          final UserJobInfo userJobInfo,
                          final UserParameters userParameters)
            throws IllegalAccessException, InstantiationException,
            NoSuchMethodException, InvocationTargetException {
        this.groupCommDriver = groupCommDriver;
        this.dataLoadingService = dataLoadingService;
        this.userJobInfo = userJobInfo;
        this.stageInfoList = userJobInfo.getStageInfoList();
        this.commGroupDriverList = new LinkedList<>();
        this.contextToStageSequence = new HashMap<>();
        this.userParameters = userParameters;
        initializeCommDriver();


    }

    /**
     * Initialize the group communication driver
     */
    private void initializeCommDriver(){

        int sequence = 0;

        for (StageInfo stageInfo : stageInfoList) {

            CommunicationGroupDriver commGroup = groupCommDriver.newCommunicationGroup(
                    stageInfo.getCommGroupName(),
                    dataLoadingService.getNumberOfPartitions() + 1);

            commGroup.addBroadcast(CtrlMsgBroadcast.class,
                    BroadcastOperatorSpec.newBuilder()
                            .setSenderId(getCtrlTaskId(sequence))
                            .setDataCodecClass(SerializableCodec.class)
                            .build());

            if (stageInfo.isBroadcastUsed()) {
                commGroup.addBroadcast(DataBroadcast.class,
                        BroadcastOperatorSpec.newBuilder()
                                .setSenderId(getCtrlTaskId(sequence))
                                .setDataCodecClass(stageInfo.getBroadcastCodecClass())
                                .build());
            }

            if (stageInfo.isScatterUsed()) {
                commGroup.addScatter(DataScatter.class,
                        ScatterOperatorSpec.newBuilder()
                                .setSenderId(getCtrlTaskId(sequence))
                                .setDataCodecClass(stageInfo.getScatterCodecClass())
                                .build());
            }

            if (stageInfo.isReduceUsed()) {
                commGroup.addReduce(DataReduce.class,
                        ReduceOperatorSpec.newBuilder()
                                .setReceiverId(getCtrlTaskId(sequence))
                                .setDataCodecClass(stageInfo.getReduceCodecClass())
                                .setReduceFunctionClass(stageInfo.getReduceFunctionClass())
                                .build());
            }

            if (stageInfo.isGatherUsed()) {
                commGroup.addGather(DataGather.class,
                        GatherOperatorSpec.newBuilder()
                                .setReceiverId(getCtrlTaskId(sequence))
                                .setDataCodecClass(stageInfo.getGatherCodecClass())
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

            // Evaluator configured with a Data Loading context has been given
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
                    finalServiceConf = Configurations.merge(userParameters.getServiceConf(), groupCommServiceConf, keyValueStoreConf);

                } else {
                    LOG.log(Level.INFO, "Submitting GroupCommContext for ComputeTask to underlying context");

                    // Add a Data Parse service and a Key-Value Store service with the Group Communication service
                    final Configuration dataParseConf = DataParseService.getServiceConfiguration(userJobInfo.getDataParser());
                    final Configuration keyValueStoreConf = KeyValueStoreService.getServiceConfiguration();
                    finalServiceConf = Configurations.merge(userParameters.getServiceConf(), groupCommServiceConf, dataParseConf, keyValueStoreConf);
                }

                activeContext.submitContextAndService(groupCommContextConf, finalServiceConf);

            } else {
                submitTask(activeContext, 0);
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

    /**
     * Receives metrics from compute tasks
     */
    final class TaskMessageHandler implements EventHandler<TaskMessage> {
        @Override
        public void onNext(final TaskMessage message) {
            final long result = codecLong.decode(message.get());
            final String msg = "Task message " + message.getId() + ": " + result;

            //TODO: use metric to run optimization plan
            LOG.info(msg);
        }
    }

    /**
     * When a certain task completes, the following task is submitted
     */
    final class TaskCompletedHandler implements EventHandler<CompletedTask> {

        @Override
        public void onNext(CompletedTask completedTask) {
            LOG.info(completedTask.getId() + " has completed.");

            ActiveContext activeContext = completedTask.getActiveContext();
            String contextId = activeContext.getId();
            int nextSequence = contextToStageSequence.get(contextId)+1;
            if (nextSequence >= stageInfoList.size()) {
                completedTask.getActiveContext().close();
                return;
            } else {
                submitTask(activeContext, nextSequence);
            }

        }
    }

    /**
     * When a certain Compute Task fails, we add the Task back and let it participate in
     * Group Communication again. However if the failed Task is the Controller Task,
     * we just shut down the whole job because it's hard to recover the cluster centroid info.
     */
    /*final class FailedTaskHandler implements EventHandler<FailedTask> {
        @Override
        public void onNext(FailedTask failedTask) {
            LOG.info(failedTask.getId() + " has failed.");

            // Stop the whole job if the failed Task is the Controller Task
            if (isCtrlTaskId(failedTask.getActiveContext().get().getId())) {
                throw new RuntimeException("Controller Task failed; aborting job");
            } else {

                ActiveContext activeContext = failedTask.getActiveContext().get();
                String contextId = activeContext.getId();
                int currentSequence = contextToStageSequence.get(contextId);
                submitTask(activeContext, currentSequence);
            }

        }
    }*/


    /**
     * Execute the task of the given stage
     * @param activeContext
     * @param stageSequence
     */
    final private void submitTask(ActiveContext activeContext, int stageSequence) {

        contextToStageSequence.put(activeContext.getId(), stageSequence);
        StageInfo taskInfo = stageInfoList.get(stageSequence);
        CommunicationGroupDriver commGroup = commGroupDriverList.get(stageSequence);
        final Configuration partialTaskConf;

        // Case 1: Evaluator configured with a Group Communication context has been given,
        //         representing a Controller Task
        // We can now place a Controller Task on top of the contexts.
        if (isCtrlTaskId(activeContext.getId())) {
            LOG.log(Level.INFO, "Submit ControllerTask");
            partialTaskConf = Configurations.merge(
                    TaskConfiguration.CONF
                            .set(TaskConfiguration.IDENTIFIER, getCtrlTaskId(stageSequence))
                            .set(TaskConfiguration.TASK, ControllerTask.class)
                            .build(),
                    Tang.Factory.getTang().newConfigurationBuilder()
                            .bindImplementation(UserControllerTask.class, taskInfo.getUserCtrlTaskClass())
                            .bindNamedParameter(CommunicationGroup.class, taskInfo.getCommGroupName().getName())
                            .build(),
                    userParameters.getUserCtrlTaskConf());

        // Case 2: Evaluator configured with a Group Communication context has been given,
        //         representing a Compute Task
        // We can now place a Compute Task on top of the contexts.
        } else {
            LOG.log(Level.INFO, "Submit ComputeTask");
            partialTaskConf = Configurations.merge(
                    TaskConfiguration.CONF
                            .set(TaskConfiguration.IDENTIFIER, getCmpTaskId(taskId.getAndIncrement()))
                            .set(TaskConfiguration.TASK, ComputeTask.class)
                            .set(TaskConfiguration.ON_SEND_MESSAGE, ComputeTask.class)
                            .build(),
                    Tang.Factory.getTang().newConfigurationBuilder()
                            .bindImplementation(UserComputeTask.class, taskInfo.getUserCmpTaskClass())
                            .bindNamedParameter(CommunicationGroup.class, taskInfo.getCommGroupName().getName())
                            .build(),
                    userParameters.getUserCmpTaskConf());
        }

        // add the Task to our communication group
        commGroup.addTask(partialTaskConf);
        final Configuration finalTaskConf = groupCommDriver.getTaskConfiguration(partialTaskConf);
        activeContext.submitTask(finalTaskConf);
    }

    final private boolean isCtrlTaskId(String id){
        if (ctrlTaskContextId==null) {
            return false;
        } else {
            return ctrlTaskContextId.equals(id);
        }
    }

    final private String getCtrlTaskId(int sequence) {
        return ControllerTask.TASK_ID + "-" + sequence;
    }

    final private String getCmpTaskId(int sequence) {
        return ComputeTask.TASK_ID + "-" + sequence;
    }




}
