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
  static final class NumClasses implements Name<Integer> {
  }

  @NamedParameter(doc = "input dimension", short_name = "features")
  static final class NumFeatures implements Name<Integer> {
  }

  @NamedParameter(doc = "initial value of the step size", short_name = "init_step_size")
  static final class InitialStepSize implements Name<Double> {
  }

  @NamedParameter(doc = "regularization constant", short_name = "lambda")
  static final class Lambda implements Name<Double> {
  }

  @NamedParameter(doc = "number of features for each model partition",
      short_name = "features_per_partition")
  static final class NumFeaturesPerPartition implements Name<Integer> {
  }

  @NamedParameter(doc = "standard deviation of the gaussian distribution used for initializing model parameters",
      short_name = "model_gaussian",
      default_value = "0.001")
  static final class ModelGaussian implements Name<Double> {
  }

  @NamedParameter(doc = "number of iterations to wait until learning wait decreases (periodic)",
      short_name = "decay_period")
  static final class DecayPeriod implements Name<Integer> {
  }

  @NamedParameter(doc = "ratio which learning rate decreases by (multiplicative)",
      short_name = "decay_rate")
  static final class DecayRate implements Name<Double> {
  }

  @NamedParameter(doc = "size of the dataset used for measuring training loss",
      short_name = "train_error_dataset_size")
  static final class TrainErrorDatasetSize implements Name<Integer> {
  }

  static final class MetricKeys {

    // The key denoting the average of loss computed from the sample of training data instances.
    static final String SAMPLE_LOSS_AVG =
        "MLR_WORKER_SAMPLE_LOSS_AVG";

    // The key denoting the average of L2-regularized loss computed from the sample of training data instances.
    static final String REG_LOSS_AVG =
        "MLR_WORKER_REG_LOSS_AVG";

    // The key denoting the number of training data instances processed per unit time.
    static final String DVT =
        "MLR_WORKER_DVT";

    // The key denoting accuracy (the number of correct inferences by the model / total number of training data inst).
    static final String ACCURACY =
        "MLR_WORKER_ACCURACY";

    // The key denoting the number of training data instances processed since the last batch.
    static final String NUM_INSTANCES =
        "MLR_WORKER_NUM_INSTANCES";
  }
}
