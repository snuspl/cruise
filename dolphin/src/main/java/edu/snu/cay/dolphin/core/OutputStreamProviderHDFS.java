/*
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
package edu.snu.cay.dolphin.core;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Implementation of {@link OutputStreamProvider} which provides FileOutputStreams on HDFS.
 */
public final class OutputStreamProviderHDFS implements OutputStreamProvider {

  /**
   * Path of the output directory on HDFS to write outputs.
   */
  private final String outputPath;

  /**
   * Id of the current task.
   */
  private String taskId;

  /**
   * HDFS File system.
   */
  private FileSystem fs;

  @Inject
  private OutputStreamProviderHDFS(
      @Parameter(OutputService.OutputPath.class) final String outputPath) throws IOException {
    this.outputPath = outputPath;
    final JobConf jobConf = new JobConf();
    fs = FileSystem.get(jobConf);
  }

  @Override
  public DataOutputStream create(final String name) throws IOException {
    final String directoryPath = outputPath + Path.SEPARATOR + name;
    if (!fs.exists(new Path(directoryPath))) {
      fs.mkdirs(new Path(directoryPath));
    }
    return fs.create(new Path(directoryPath + Path.SEPARATOR + taskId));
  }

  @Override
  public void close() throws IOException {
    fs.close();
  }

  @Override
  public void setTaskId(final String taskId) {
    this.taskId = taskId;
  }
}
