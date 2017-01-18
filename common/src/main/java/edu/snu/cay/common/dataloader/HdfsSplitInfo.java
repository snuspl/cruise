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
package edu.snu.cay.common.dataloader;

import org.apache.hadoop.mapred.InputSplit;

/**
 * Contains information to fetch a split from HDFS.
 * It should be created in Driver and used in Evaluator.
 */
public final class HdfsSplitInfo {
  private final String inputPath;
  private final InputSplit inputSplit;
  private final String inputFormatClassName;

  public HdfsSplitInfo(final String inputPath, final InputSplit inputSplit, final String inputFormatClassName) {
    this.inputPath = inputPath;
    this.inputSplit = inputSplit;
    this.inputFormatClassName = inputFormatClassName;
  }

  /**
   * @return input path
   */
  public String getInputPath() {
    return inputPath;
  }

  /**
   * @return {@link InputSplit}
   */
  public InputSplit getInputSplit() {
    return inputSplit;
  }

  /**
   * @return the name of InputFormat class
   */
  public String getInputFormatClassName() {
    return inputFormatClassName;
  }
}
