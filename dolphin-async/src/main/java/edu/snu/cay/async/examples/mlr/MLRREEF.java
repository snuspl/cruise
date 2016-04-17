/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.async.examples.mlr;

import edu.snu.cay.async.AsyncDolphinConfiguration;
import edu.snu.cay.async.AsyncDolphinLauncher;
import edu.snu.cay.async.examples.nmf.DenseVectorCodec;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Application launching code for MLRREEF.
 */
public final class MLRREEF {

  /**
   * Should not be instantiated.
   */
  private MLRREEF() {
  }

  public static void main(final String[] args) {
    AsyncDolphinLauncher.launch("MLRREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setWorkerClass(MLRWorker.class)
        .setUpdaterClass(MLRUpdater.class)
        .setPreValueCodecClass(DenseVectorCodec.class)
        .setValueCodecClass(DenseVectorCodec.class)
        .addParameterClass(NumClasses.class)
        .addParameterClass(NumFeatures.class)
        .addParameterClass(BatchSize.class)
        .addParameterClass(StepSize.class)
        .addParameterClass(Lambda.class)
        .addParameterClass(LossLogPeriod.class)
        .addParameterClass(NumFeaturesPerPartition.class)
        .build());
  }

  @NamedParameter(doc = "number of classes", short_name = "classes")
  final class NumClasses implements Name<Integer> {
  }

  @NamedParameter(doc = "input dimension", short_name = "features")
  final class NumFeatures implements Name<Integer> {
  }

  @NamedParameter(doc = "number of instances for each mini-batch", short_name = "batch")
  final class BatchSize implements Name<Integer> {
  }

  @NamedParameter(doc = "initial value of the step size", short_name = "stepSize")
  final class StepSize implements Name<Double> {
  }

  @NamedParameter(doc = "regularization constant", short_name = "lambda")
  final class Lambda implements Name<Double> {
  }

  @NamedParameter(doc = "number of iterations to wait until logging the loss value",
                  short_name = "lossLogPeriod",
                  default_value = "0")
  final class LossLogPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "number of features for each model partition",
                  short_name = "featuresPerPartition")
  final class NumFeaturesPerPartition implements Name<Integer> {
  }
}
