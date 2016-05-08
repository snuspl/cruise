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
        .addParameterClass(InitialStepSize.class)
        .addParameterClass(Lambda.class)
        .addParameterClass(StatusLogPeriod.class)
        .addParameterClass(NumFeaturesPerPartition.class)
        .addParameterClass(ModelGaussian.class)
        .addParameterClass(DecayPeriod.class)
        .addParameterClass(DecayRate.class)
        .addParameterClass(TrainErrorDatasetSize.class)
        .addParameterClass(NumBatchPerLossLog.class)
        .addParameterClass(NumBatchPerIter.class)
        .build());
  }

  @NamedParameter(doc = "number of classes", short_name = "classes")
  final class NumClasses implements Name<Integer> {
  }

  @NamedParameter(doc = "input dimension", short_name = "features")
  final class NumFeatures implements Name<Integer> {
  }

  @NamedParameter(doc = "initial value of the step size", short_name = "initStepSize")
  final class InitialStepSize implements Name<Double> {
  }

  @NamedParameter(doc = "regularization constant", short_name = "lambda")
  final class Lambda implements Name<Double> {
  }

  @NamedParameter(doc = "number of iterations to wait until logging the current status",
                  short_name = "statusLogPeriod",
                  default_value = "0")
  final class StatusLogPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "number of features for each model partition",
                  short_name = "featuresPerPartition")
  final class NumFeaturesPerPartition implements Name<Integer> {
  }

  @NamedParameter(doc = "standard deviation of the gaussian distribution used for initializing model parameters",
                  short_name = "modelGaussian",
                  default_value = "0.001")
  final class ModelGaussian implements Name<Double> {
  }

  @NamedParameter(doc = "number of iterations to wait until learning wait decreases (periodic)",
                  short_name = "decayPeriod")
  final class DecayPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "ratio which learning rate decreases by (multiplicative)",
                  short_name = "decayRate")
  final class DecayRate implements Name<Double> {
  }

  @NamedParameter(doc = "size of the dataset used for measuring training loss",
                  short_name = "trainErrorDatasetSize")
  final class TrainErrorDatasetSize implements Name<Integer> {
  }

  @NamedParameter(doc = "log the current loss after this many mini-batches",
                  short_name = "numBatchPerLossLog")
  final class NumBatchPerLossLog implements Name<Integer> {
  }

  @NamedParameter(doc = "number of mini-batches per iteration",
                  short_name = "numBatchPerIter")
  final class NumBatchPerIter implements Name<Integer> {
  }
}
