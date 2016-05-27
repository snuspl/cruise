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
package edu.snu.cay.dolphin.async.examples.lasso;

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Application launching code for LassoREEF.
 */
public final class LassoREEF {

  /**
   * Should not be instantiated.
   */
  private LassoREEF() {
  }

  public static void main(final String[] args) {
    AsyncDolphinLauncher.launch("LassoREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setWorkerClass(LassoWorker.class)
        .setUpdaterClass(LassoUpdater.class)
        .addParameterClass(NumFeatures.class)
        .addParameterClass(Lambda.class)
        .build());
  }

  @NamedParameter(doc = "input dimension", short_name = "features")
  final class NumFeatures implements Name<Integer> {
  }

  @NamedParameter(doc = "regularization constant", short_name = "lambda")
  final class Lambda implements Name<Double> {
  }
}
