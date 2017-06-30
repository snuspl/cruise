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
 * Created by xyzi on 30/06/2017.
 */
final class Parameters {
  static final String SUBMIT_COMMAMD = "submit";
  static final String FINISH_COMMAND = "finish";

  private Parameters() {

  }

  @NamedParameter(doc = "A port number of HTTP request.", short_name = "port")
  final class HttpPort implements Name<String> {

  }

  @NamedParameter(doc = "An address of HTTP request", short_name = "address")
  final class HttpAddress implements Name<String> {

  }

  @NamedParameter(doc = "An identifier of App.")
  final class AppIdentifier implements Name<String> {

  }
}
