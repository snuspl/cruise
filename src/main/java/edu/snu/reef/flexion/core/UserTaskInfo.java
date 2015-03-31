package edu.snu.reef.flexion.core;


import com.microsoft.reef.io.network.group.operators.Reduce;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;

/**
 * Information of a task, which corresponds to a BSP algorithm
 * One or more tasks compose a job, a unit of work in Flexion
 */
public final class UserTaskInfo {

    private Class<? extends UserComputeTask> userComputeTaskClass;
    private Class<? extends UserControllerTask> userControllerTaskClass;
    private Class<? extends Name<String>> commGroupName;

    private boolean isBroadcastUsed = false;
    private Class<? extends Codec>  broadcastCodecClass = null;

    private boolean isScatterUsed = false;
    private Class<? extends Codec>  scatterCodecClass = null;

    private boolean isGatherUsed = false;
    private Class<? extends Codec>  gatherCodecClass = null;

    private boolean isReduceUsed = false;
    private Class<? extends Codec>  reduceCodecClass = null;
    private Class<? extends Reduce.ReduceFunction> reduceFunctionClass = null;

    /**
     * Define a new task
     * @param userComputeTaskClass  user-defined compute task
     * @param userControllerTaskClass   user-defined controller task
     * @param communicationGroup    name of the communication group used by this task
     */
    public UserTaskInfo(Class<? extends UserComputeTask> userComputeTaskClass,
                        Class<? extends UserControllerTask> userControllerTaskClass,
                        Class<? extends Name<String>> communicationGroup) {
        this.userComputeTaskClass = userComputeTaskClass;
        this.userControllerTaskClass = userControllerTaskClass;
        this.commGroupName = communicationGroup;
    }

    public UserTaskInfo setBroadcast(final Class<? extends Codec> codecClass) {
        this.isBroadcastUsed = true;
        this.broadcastCodecClass = codecClass;
        return this;
    }

    public UserTaskInfo setScatter(final Class<? extends Codec> codecClass) {
        this.isScatterUsed = true;
        this.scatterCodecClass = codecClass;
        return this;
    }

    public UserTaskInfo setGather(final Class<? extends Codec> codecClass) {
        this.isGatherUsed = true;
        this.gatherCodecClass = codecClass;
        return this;
    }

    public UserTaskInfo setReduce(final Class<? extends Codec> codecClass,
                              final Class<? extends Reduce.ReduceFunction> reduceFunctionClass) {
        this.isReduceUsed = true;
        this.reduceCodecClass = codecClass;
        this.reduceFunctionClass = reduceFunctionClass;
        return this;
    }

    public boolean isBroadcastUsed(){
        return this.isBroadcastUsed;
    }

    public boolean isScatterUsed(){
        return this.isScatterUsed;
    }

    public boolean isGatherUsed(){
        return this.isGatherUsed;
    }

    public boolean isReduceUsed(){
        return this.isReduceUsed;
    }

    public Class<? extends Codec> getBroadcastCodecClass() {
        return this.broadcastCodecClass;
    }

    public Class<? extends Codec> getScatterCodecClass() {
        return this.scatterCodecClass;
    }

    public Class<? extends Codec> getGatherCodecClass() {
        return this.gatherCodecClass;
    }

    public Class<? extends Codec> getReduceCodecClass() {
        return this.reduceCodecClass;
    }

    public Class<? extends Reduce.ReduceFunction> getReduceFunctionClass() {
        return this.reduceFunctionClass;
    }

    public Class<? extends UserComputeTask> getUserCmpTaskClass() {
        return this.userComputeTaskClass;
    }

    public Class<? extends UserControllerTask> getUserCtrlTaskClass() {
        return this.userControllerTaskClass;
    }

    public Class<? extends Name<String>> getCommGroupName() {
        return this.commGroupName;
    }
}
