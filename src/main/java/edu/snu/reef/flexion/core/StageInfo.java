package edu.snu.reef.flexion.core;


import com.microsoft.reef.io.network.group.operators.Reduce;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;

/**
 * Information of a stage, which corresponds to a BSP algorithm
 * One or more stages compose a job, a unit of work in Flexion
 */
public final class StageInfo {

  private final Class<? extends UserComputeTask> userComputeTaskClass;
  private final Class<? extends UserControllerTask> userControllerTaskClass;
  private final Class<? extends Name<String>> commGroupName;

  private final boolean isBroadcastUsed;
  private final Class<? extends Codec> broadcastCodecClass;

  private final boolean isScatterUsed;
  private final Class<? extends Codec> scatterCodecClass;

  private final boolean isGatherUsed;
  private final Class<? extends Codec> gatherCodecClass;

  private final boolean isReduceUsed;
  private final Class<? extends Codec> reduceCodecClass;
  private final Class<? extends Reduce.ReduceFunction> reduceFunctionClass;

  public static Builder newBuilder(Class<? extends UserComputeTask> userComputeTaskClass,
                                   Class<? extends UserControllerTask> userControllerTaskClass,
                                   Class<? extends Name<String>> communicationGroup) {
    return new StageInfo.Builder(userComputeTaskClass, userControllerTaskClass, communicationGroup);
  }

  public StageInfo(Class<? extends UserComputeTask> userComputeTaskClass,
                   Class<? extends UserControllerTask> userControllerTaskClass,
                   Class<? extends Name<String>> communicationGroup,
                   boolean isBroadcastUsed,
                   Class<? extends Codec> broadcastCodecClass,
                   boolean isScatterUsed,
                   Class<? extends Codec> scatterCodecClass,
                   boolean isGatherUsed,
                   Class<? extends Codec> gatherCodecClass,
                   boolean isReduceUsed,
                   Class<? extends Codec> reduceCodecClass,
                   Class<? extends Reduce.ReduceFunction> reduceFunctionClass) {
    this.userComputeTaskClass = userComputeTaskClass;
    this.userControllerTaskClass = userControllerTaskClass;
    this.commGroupName = communicationGroup;
    this.isBroadcastUsed = isBroadcastUsed;
    this.broadcastCodecClass = broadcastCodecClass;
    this.isScatterUsed = isScatterUsed;
    this.scatterCodecClass = scatterCodecClass;
    this.isGatherUsed = isGatherUsed;
    this.gatherCodecClass = gatherCodecClass;
    this.isReduceUsed = isReduceUsed;
    this.reduceCodecClass = reduceCodecClass;
    this.reduceFunctionClass = reduceFunctionClass;
  }

  public static class Builder implements org.apache.reef.util.Builder<StageInfo> {

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
     * @param userComputeTaskClass  user-defined compute task
     * @param userControllerTaskClass   user-defined controller task
     * @param communicationGroup    name of the communication group used by this stage
     */
    public Builder(Class<? extends UserComputeTask> userComputeTaskClass,
                   Class<? extends UserControllerTask> userControllerTaskClass,
                   Class<? extends Name<String>> communicationGroup) {
      this.userComputeTaskClass = userComputeTaskClass;
      this.userControllerTaskClass = userControllerTaskClass;
      this.commGroupName = communicationGroup;
    }

    public Builder setBroadcast(final Class<? extends Codec> codecClass) {
      this.isBroadcastUsed = true;
      this.broadcastCodecClass = codecClass;
      return this;
    }

    public Builder setScatter(final Class<? extends Codec> codecClass) {
      this.isScatterUsed = true;
      this.scatterCodecClass = codecClass;
      return this;
    }

    public Builder setGather(final Class<? extends Codec> codecClass) {
      this.isGatherUsed = true;
      this.gatherCodecClass = codecClass;
      return this;
    }

    public Builder setReduce(final Class<? extends Codec> codecClass,
                             final Class<? extends Reduce.ReduceFunction> reduceFunctionClass) {
      this.isReduceUsed = true;
      this.reduceCodecClass = codecClass;
      this.reduceFunctionClass = reduceFunctionClass;
      return this;
    }

    @Override
    public StageInfo build() {
      return new StageInfo(userComputeTaskClass, userControllerTaskClass, commGroupName,
          isBroadcastUsed, broadcastCodecClass,
          isScatterUsed, scatterCodecClass,
          isGatherUsed, gatherCodecClass,
          isReduceUsed, reduceCodecClass, reduceFunctionClass);
    }
  }


  public boolean isBroadcastUsed() {
    return this.isBroadcastUsed;
  }

  public boolean isScatterUsed() {
    return this.isScatterUsed;
  }

  public boolean isGatherUsed() {
    return this.isGatherUsed;
  }

  public boolean isReduceUsed() {
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
