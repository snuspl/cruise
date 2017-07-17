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

  private Parameters() {

  }

  @NamedParameter(doc = "An identifier of App.")
  final class AppIdentifier implements Name<String> {

  }

  /**
   * Adress for job server commands.
   */
  @NamedParameter(doc = "Address for job server commands",
      short_name = "address", default_value = "localhost")
  final class Address implements Name<String> {

  }

  /**
   * Port for job server commands.
   */
  @NamedParameter(doc = "Port for job server commands",
      short_name = "port", default_value = "7008")
  final class Port implements Name<Integer> {

  }
}
