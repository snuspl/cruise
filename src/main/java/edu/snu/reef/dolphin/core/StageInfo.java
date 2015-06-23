/**
 * Copyright (C) 2015 Seoul National University
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package edu.snu.reef.dolphin.core;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.reef.dolphin.core.metric.MetricTracker;
import org.apache.reef.io.serialization.Codec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.util.Optional;

import java.util.HashSet;
import java.util.Set;

/**
 * Information of a stage, which corresponds to a BSP algorithm
 * One or more stages compose a job, a unit of work in Dolphin
 */
public final class StageInfo {
  private final Class<? extends UserComputeTask> userComputeTaskClass;
  private final Class<? extends UserControllerTask> userControllerTaskClass;
  private final Class<? extends Name<String>> commGroupName;

  private final Optional<? extends Class<? extends Codec>> broadcastCodecClassOptional;
  private final Optional<? extends Class<? extends Codec>> scatterCodecClassOptional;
  private final Optional<? extends Class<? extends Codec>> gatherCodecClassOptional;
  private final Optional<? extends Class<? extends Codec>> reduceCodecClassOptional;
  private final Optional<? extends Class<? extends Reduce.ReduceFunction>> reduceFunctionClassOptional;
  private final Optional<Set<Class<? extends MetricTracker>>> metricTrackerClassSet;

  public static Builder newBuilder(final Class<? extends UserComputeTask> userComputeTaskClass,
                                   final Class<? extends UserControllerTask> userControllerTaskClass,
                                   final Class<? extends Name<String>> communicationGroup) {
    return new StageInfo.Builder(userComputeTaskClass, userControllerTaskClass, communicationGroup);
  }

  private StageInfo(final Class<? extends UserComputeTask> userComputeTaskClass,
                    final Class<? extends UserControllerTask> userControllerTaskClass,
                    final Class<? extends Name<String>> communicationGroup,
                    final Class<? extends Codec> broadcastCodecClass,
                    final Class<? extends Codec> scatterCodecClass,
                    final Class<? extends Codec> gatherCodecClass,
                    final Class<? extends Codec> reduceCodecClass,
                    final Class<? extends Reduce.ReduceFunction> reduceFunctionClass,
                    final Set<Class<? extends MetricTracker>> metricTrackerClassSet) {
    this.userComputeTaskClass = userComputeTaskClass;
    this.userControllerTaskClass = userControllerTaskClass;
    this.commGroupName = communicationGroup;
    this.broadcastCodecClassOptional = Optional.ofNullable(broadcastCodecClass);
    this.scatterCodecClassOptional = Optional.ofNullable(scatterCodecClass);
    this.gatherCodecClassOptional = Optional.ofNullable(gatherCodecClass);
    this.reduceCodecClassOptional = Optional.ofNullable(reduceCodecClass);
    this.reduceFunctionClassOptional = Optional.ofNullable(reduceFunctionClass);
    this.metricTrackerClassSet = Optional.ofNullable(metricTrackerClassSet);
  }

  public boolean isBroadcastUsed() {
    return broadcastCodecClassOptional.isPresent();
  }

  public boolean isScatterUsed() {
    return scatterCodecClassOptional.isPresent();
  }

  public boolean isGatherUsed() {
    return gatherCodecClassOptional.isPresent();
  }

  public boolean isReduceUsed() {
    return reduceCodecClassOptional.isPresent();
  }

  public Class<? extends Codec> getBroadcastCodecClass() {
    return broadcastCodecClassOptional.get();
  }

  public Class<? extends Codec> getScatterCodecClass() {
    return scatterCodecClassOptional.get();
  }

  public Class<? extends Codec> getGatherCodecClass() {
    return gatherCodecClassOptional.get();
  }

  public Class<? extends Codec> getReduceCodecClass() {
    return reduceCodecClassOptional.get();
  }

  public Class<? extends Reduce.ReduceFunction> getReduceFunctionClass() {
    return reduceFunctionClassOptional.get();
  }

  public Set<Class<? extends MetricTracker>> getMetricTrackerClassSet() {
    return metricTrackerClassSet.get();
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

  public static class Builder implements org.apache.reef.util.Builder<StageInfo> {
    private Class<? extends UserComputeTask> userComputeTaskClass;
    private Class<? extends UserControllerTask> userControllerTaskClass;
    private Class<? extends Name<String>> commGroupName;
    private Class<? extends Codec> broadcastCodecClass = null;
    private Class<? extends Codec> scatterCodecClass = null;
    private Class<? extends Codec> gatherCodecClass = null;
    private Class<? extends Codec> reduceCodecClass = null;
    private Class<? extends Reduce.ReduceFunction> reduceFunctionClass = null;
    private Set<Class<? extends MetricTracker>> metricTrackerSet = null;

    /**
     * @param userComputeTaskClass  user-defined compute task
     * @param userControllerTaskClass   user-defined controller task
     * @param communicationGroup    name of the communication group used by this stage
     */
    public Builder(final Class<? extends UserComputeTask> userComputeTaskClass,
                   final Class<? extends UserControllerTask> userControllerTaskClass,
                   final Class<? extends Name<String>> communicationGroup) {
      this.userComputeTaskClass = userComputeTaskClass;
      this.userControllerTaskClass = userControllerTaskClass;
      this.commGroupName = communicationGroup;
    }

    public Builder setBroadcast(final Class<? extends Codec> codecClass) {
      this.broadcastCodecClass = codecClass;
      return this;
    }

    public Builder setScatter(final Class<? extends Codec> codecClass) {
      this.scatterCodecClass = codecClass;
      return this;
    }

    public Builder setGather(final Class<? extends Codec> codecClass) {
      this.gatherCodecClass = codecClass;
      return this;
    }

    public Builder setReduce(final Class<? extends Codec> codecClass,
                             final Class<? extends Reduce.ReduceFunction> reduceFunctionClass) {
      this.reduceCodecClass = codecClass;
      this.reduceFunctionClass = reduceFunctionClass;
      return this;
    }

    public Builder addMetricTrackers(final Class<? extends MetricTracker>... metricTrackerClasses) {
      metricTrackerSet = new HashSet<>();
      for(int i = 0; i < metricTrackerClasses.length; i++) {
        this.metricTrackerSet.add(metricTrackerClasses[i]);
      }
      return this;
    }

    @Override
    public StageInfo build() {
      return new StageInfo(userComputeTaskClass, userControllerTaskClass, commGroupName,
          broadcastCodecClass, scatterCodecClass, gatherCodecClass, reduceCodecClass, reduceFunctionClass,
          metricTrackerSet);
    }
  }
}
