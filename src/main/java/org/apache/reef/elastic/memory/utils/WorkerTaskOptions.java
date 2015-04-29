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

package org.apache.reef.elastic.memory.utils;

import org.apache.reef.tang.annotations.Name;
import org.apache.reef.tang.annotations.NamedParameter;

import java.util.Set;

/**
 * NamedParameters for WorkerTask
 */
public class WorkerTaskOptions {
  @NamedParameter(doc = "Index of split string to be printed on WorkerTask ")
  public static class IndexOfWord implements Name<Integer> {
  }
  @NamedParameter(doc = "String task IDs of destinations for NetworkServiceWrapper")
  public static class Destinations implements Name<Set<String>> {
  }
}
