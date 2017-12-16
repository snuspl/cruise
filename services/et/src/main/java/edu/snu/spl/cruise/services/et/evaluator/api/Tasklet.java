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
package edu.snu.spl.cruise.services.et.evaluator.api;

/**
 * The interface for Tasklets.
 * When a tasklet has been submitted, executor runtime execute it by calling {@link #run()}.
 * When master directs the tasklet to stop the tasklet, executor runtime will call {@link #close()}.
 */
public interface Tasklet {

  /**
   * Run a tasklet.
   * @throws Exception when tasklet encounters unresolved issues
   */
  void run() throws Exception;

  /**
   * Close the running tasklet.
   */
  void close();
}
