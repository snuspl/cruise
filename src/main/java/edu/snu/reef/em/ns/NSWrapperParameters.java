/**
 * Copyright (C) 2014 Seoul National University
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

package edu.snu.reef.em.ns;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

/**
 * Named parameters for NetworkServiceWrappers
 */
public class NSWrapperParameters {
  @NamedParameter(doc = "Name server address for NetworkServiceWrapper")
  public static class NameServerAddr implements Name<String> {
  }
  @NamedParameter(doc = "Name server port for NetworkServiceWrapper")
  public static class NameServerPort implements Name<Integer> {
  }
}
