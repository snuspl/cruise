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
package edu.snu.cay.dolphin.async.jobserver;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Parameters used in JobServer.
 */
final class Parameters {
  static final String SUBMIT_COMMAND = "SUBMIT";
  static final String SHUTDOWN_COMMAND = "SHUTDOWN";
  static final String COMMAND_DELIMITER = " ";

  static final int PORT_NUMBER = 7008;

  private Parameters() {

  }

  @NamedParameter(doc = "An identifier of App.")
  final class AppIdentifier implements Name<String> {

  }

  @NamedParameter(doc = "The number of total available resources in a cluster",
      short_name = "num_total_resources")
  final class NumTotalResources implements Name<Integer> {
  }

  @NamedParameter(doc = "A class of the scheduler",
      short_name = "scheduler",
      default_value = "edu.snu.cay.dolphin.async.jobserver.FIFOJobScheduler")
  final class SchedulerClass implements Name<String> {
  }
}
