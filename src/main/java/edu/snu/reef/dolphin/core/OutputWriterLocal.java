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


import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * Implementation of {@link OutputWriter} for an output file on the local disk
 */
public final class OutputWriterLocal implements OutputWriter {

  /**
   * Path of the output file on the local disk to write outputs
   */
  private final String outputPath;

  /**
   * Output stream to the output file on the local disk
   */
  private DataOutputStream outputStream;

  @Inject
  private OutputWriterLocal(
      @Parameter(OutputService.OutputPath.class) String outputPath) {
    this.outputPath = outputPath;
  }

  @Override
  public void create() throws IOException {
    File file = new File(outputPath);
    file.getParentFile().mkdirs();
    outputStream = new DataOutputStream(new FileOutputStream(file));
  }

  @Override
  public void write(final String string) throws IOException {
    outputStream.writeUTF(string);
  }

  @Override
  public void close() throws IOException {
    outputStream.close();
  }
}
