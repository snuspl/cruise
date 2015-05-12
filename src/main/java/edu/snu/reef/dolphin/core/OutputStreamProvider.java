/**
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.reef.dolphin.core;

import java.io.DataOutputStream;
import java.io.IOException;

/**
 * A provider through which users create output streams
 */
public interface OutputStreamProvider {

  /**
   * initialize provider
   * @throws IOException
   */
  public void initialize() throws IOException;

  /**
   * create an output stream using the given name
   * @param name name of the created output stream.
   *             it is used as the name of the file if the created output stream is a file output stream
   * @throws java.io.IOException
   */
  public DataOutputStream create(final String name) throws IOException;


  /**
   * release unused resources
   * @throws IOException
   */
  public void close() throws IOException;

}
