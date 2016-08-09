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
package edu.snu.cay.dolphin.async.examples.addvector;

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorCodec;
import edu.snu.cay.dolphin.async.mlapps.serialization.DenseVectorSerializer;
import org.apache.reef.client.LauncherStatus;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Application launching code for AddVectorREEF.
 */
public final class AddVectorREEF {

  /**
   * Should not be instantiated.
   */
  private AddVectorREEF() {
  }

  /**
   * Runs app with given arguments and custom configuration.
   * @param args command line arguments for running app
   * @param conf a job configuration
   * @return a LauncherStatus
   */
  public static LauncherStatus runAddVector(final String[] args, final Configuration conf) {
    return AsyncDolphinLauncher.launch("AddVectorREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setWorkerClass(AddVectorWorker.class)
        .setUpdaterClass(AddVectorUpdater.class)
        .setValueCodecClass(DenseVectorCodec.class)
        .setServerSerializerClass(DenseVectorSerializer.class)
        .addParameterClass(DeltaValue.class)
        .addParameterClass(StartKey.class)
        .addParameterClass(NumKeys.class)
        .addParameterClass(NumUpdates.class)
        .addParameterClass(NumWorkers.class)
        .addParameterClass(VectorSize.class)
        .build(), conf);
  }

  public static void main(final String[] args) {
    runAddVector(args, Tang.Factory.getTang().newConfigurationBuilder().build());
  }

  @NamedParameter(doc = "All workers will add this integer to each element of the vector", short_name = "delta")
  final class DeltaValue implements Name<Integer> {
  }

  @NamedParameter(doc = "The start key", short_name = "start_key")
  final class StartKey implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of keys", short_name = "num_keys")
  final class NumKeys implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of updates for each key in an iteration", short_name = "num_updates")
  final class NumUpdates implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of workers", short_name = "num_workers")
  final class NumWorkers implements Name<Integer> {
  }

  @NamedParameter(doc = "The size of vector", short_name = "vector_size")
  final class VectorSize implements Name<Integer> {
  }
}
