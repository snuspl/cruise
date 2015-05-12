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
 * Implementation of {@link OutputStreamProvider} which provides FileOutputStreams on the local disk
 */
public final class OutputStreamProviderLocal implements OutputStreamProvider {

  /**
   * Path of the output directory on the local disk to write outputs
   */
  private final String outputPath;

  /**
   * Id of the current evaluator
   */
  private final String evaluatorId;

  @Inject
  private OutputStreamProviderLocal(
      @Parameter(OutputService.OutputPath.class) String outputPath,
      @Parameter(OutputService.EvaluatorId.class) final String evaluatorId) {
    this.outputPath = outputPath;
    this.evaluatorId = evaluatorId;
  }

  @Override
  public void initialize() throws IOException {
    File file = new File(outputPath+File.separator+evaluatorId);
    if(!file.exists()) {
      file.mkdirs();
    }
  }

  @Override
  public DataOutputStream create(String name) throws IOException {
    File file = new File(outputPath+File.separator+evaluatorId+File.separator+name);
    return new DataOutputStream(new FileOutputStream(file));
  }

  @Override
  public void close() throws IOException {
  }
}
