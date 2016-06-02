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
        .build());
  }

  @NamedParameter(doc = "all workers will add this integer to the sum", short_name = "param")
  final class AddIntegerParameter implements Name<Integer> {
  }
}
