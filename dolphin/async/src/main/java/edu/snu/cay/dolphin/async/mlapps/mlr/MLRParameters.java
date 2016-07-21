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
package edu.snu.cay.dolphin.async.mlapps.mlr;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used for multi-class logistic regression.
 */
final class MLRParameters {

  @NamedParameter(doc = "number of classes", short_name = "classes")
  final class NumClasses implements Name<Integer> {
  }

  @NamedParameter(doc = "input dimension", short_name = "features")
  final class NumFeatures implements Name<Integer> {
  }

  @NamedParameter(doc = "initial value of the step size", short_name = "init_step_size")
  final class InitialStepSize implements Name<Double> {
  }

  @NamedParameter(doc = "regularization constant", short_name = "lambda")
  final class Lambda implements Name<Double> {
  }

  @NamedParameter(doc = "number of iterations to wait until logging the current status",
      short_name = "status_log_period",
      default_value = "0")
  final class StatusLogPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "number of features for each model partition",
      short_name = "features_per_partition")
  final class NumFeaturesPerPartition implements Name<Integer> {
  }

  @NamedParameter(doc = "standard deviation of the gaussian distribution used for initializing model parameters",
      short_name = "model_gaussian",
      default_value = "0.001")
  final class ModelGaussian implements Name<Double> {
  }

  @NamedParameter(doc = "number of iterations to wait until learning wait decreases (periodic)",
      short_name = "decay_period")
  final class DecayPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "ratio which learning rate decreases by (multiplicative)",
      short_name = "decay_rate")
  final class DecayRate implements Name<Double> {
  }

  @NamedParameter(doc = "size of the dataset used for measuring training loss",
      short_name = "train_error_dataset_size")
  final class TrainErrorDatasetSize implements Name<Integer> {
  }

  @NamedParameter(doc = "log the current loss after this many mini-batches",
      short_name = "num_batch_per_loss_log")
  final class NumBatchPerLossLog implements Name<Integer> {
  }
  static final class MetricKeys {
    // The key denoting sample avg loss.
    static final String SAMPLE_LOSS_AVG =
        "MLR_WORKER_SAMPLE_LOSS_AVG";

    // The key denoting reg avg loss.
    static final String REG_LOSS_AVG =
        "MLR_WORKER_REG_LOSS_AVG";

    // The key denoting DvT.
    static final String DVT =
        "MLR_WORKER_DVT";

    // The key denoting accuracy.
    static final String ACCURACY =
        "MLR_WORKER_ACCURACY";
  }
}
