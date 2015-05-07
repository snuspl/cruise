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

import java.io.IOException;

/**
 * An interface of an output writer, through which tasks write their output
 */
public interface OutputWriter {

  /**
   * Write the given string to the output file
   * @param string string to write
   * @throws IOException
   */
  public void write(final String string) throws IOException;

  /**
   * Create the output writer
   * @throws IOException
   */
  void create() throws IOException;

  /**
   * Close the output writer
   * @throws IOException
   */
  void close() throws IOException;
}
