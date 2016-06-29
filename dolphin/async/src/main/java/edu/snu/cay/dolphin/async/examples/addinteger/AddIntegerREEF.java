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
package edu.snu.cay.dolphin.async.examples.addinteger;

import edu.snu.cay.dolphin.async.AsyncDolphinConfiguration;
import edu.snu.cay.dolphin.async.AsyncDolphinLauncher;
import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Application launching code for AddIntegerREEF.
 */
public final class AddIntegerREEF {

  /**
   * Should not be instantiated.
   */
  private AddIntegerREEF() {
  }

  public static void main(final String[] args) {
    AsyncDolphinLauncher.launch("AddIntegerREEF", args, AsyncDolphinConfiguration.newBuilder()
        .setWorkerClass(AddIntegerWorker.class)
        .setUpdaterClass(AddIntegerUpdater.class)
        .addParameterClass(AddIntegerParameter.class)
        .addParameterClass(StartKeyParameter.class)
        .addParameterClass(NumberOfKeysParameter.class)
        .addParameterClass(NumberOfUpdatesParameter.class)
        .addParameterClass(NumberOfWorkersParameter.class)
        .build());
  }

  @NamedParameter(doc = "all workers will add this integer to the sum", short_name = "param")
  final class AddIntegerParameter implements Name<Integer> {
  }

  @NamedParameter(doc = "the start key", short_name = "start_key")
  final class StartKeyParameter implements Name<Integer> {
  }

  @NamedParameter(doc = "the number of keys", short_name = "number_of_keys")
  final class NumberOfKeysParameter implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of updates for each key in an iteration", short_name = "number_of_updates")
  final class NumberOfUpdatesParameter implements Name<Integer> {
  }

  @NamedParameter(doc = "The number of workers", short_name = "number_of_workers")
  final class NumberOfWorkersParameter implements Name<Integer> {
  }
}
