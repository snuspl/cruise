/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.dolphin.async.mlapps.gbt;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used for gradient boosting tree.
 */
final class GBTParameters {

  @NamedParameter(doc = "input dimension", short_name = "features")
  static final class NumFeatures implements Name<Integer> {
  }

  @NamedParameter(doc = "initial value of the step size", short_name = "init_step_size")
  static final class StepSize implements Name<Double> {
  }

  @NamedParameter(doc = "regularization constant for leaf", short_name = "lambda")
  static final class Lambda implements Name<Double> {
  }

  @NamedParameter(doc = "regularization constant for tree", short_name = "gamma")
  static final class Gamma implements Name<Double> {
  }

  @NamedParameter(doc = "maximum depth of model trees", short_name = "max_depth_of_tree")
  static final class TreeMaxDepth implements Name<Integer> {
  }

  @NamedParameter(doc = "minimum size of model tree's leaf", short_name = "leaf_min_size")
  static final class LeafMinSize implements Name<Integer> {
  }

  @NamedParameter(doc = "the location of metadata for features", short_name = "metadata_path")
  static final class MetadataPath implements Name<String> {
  }

  @NamedParameter(doc = "the number of keys that are assigned to each tree for partitioning models across servers",
      short_name = "num_keys")
  static final class NumKeys implements Name<Integer> {
  }
}
