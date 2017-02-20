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

import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.reef.annotations.audience.DriverSide;
import org.apache.reef.io.data.loading.impl.JobConfExternalConstructor;
import org.apache.reef.tang.Tang;
import org.apache.reef.tang.exceptions.InjectionException;

import java.io.IOException;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Used to get splits of a HDFS directory/file.
 */
public final class HdfsSplitManager {
  private static final Logger LOG = Logger.getLogger(HdfsSplitManager.class.getName());

  // utility class should not be instantiated
  private HdfsSplitManager() {
  }

  /**
   * @param path of a HDFS directory/file
   * @param inputFormatClassName a class name of {@link org.apache.hadoop.mapred.InputFormat}
   * @param numOfSplits of the directory/file
   * @return an array of HDFS split representations
   */
  public static HdfsSplitInfo[] getSplits(final String path, final String inputFormatClassName, final int numOfSplits) {
    final JobConf jobConf;
    try {
      final Tang tang = Tang.Factory.getTang();
      jobConf = tang.newInjector(
          tang.newConfigurationBuilder()
              .bindNamedParameter(JobConfExternalConstructor.InputFormatClass.class, inputFormatClassName)
              .bindNamedParameter(JobConfExternalConstructor.InputPath.class, path)
              .bindConstructor(JobConf.class, JobConfExternalConstructor.class)
              .build())
          .getInstance(JobConf.class);
    } catch (final InjectionException e) {
      throw new RuntimeException("Exception while injecting JobConf", e);
    }

    final InputSplit[] inputSplits;
    try {
      final InputFormat inputFormat = jobConf.getInputFormat();
      inputSplits = inputFormat.getSplits(jobConf, numOfSplits);
      LOG.log(Level.FINEST, "Splits: {0}", Arrays.toString(inputSplits));
    } catch (final IOException e) {
      throw new RuntimeException("Unable to get InputSplits using the specified InputFormat", e);
    }

    final HdfsSplitInfo[] hdfsSplitInfos = new HdfsSplitInfo[inputSplits.length];
    for (int idx = 0; idx < inputSplits.length; idx++) {
      hdfsSplitInfos[idx] = HdfsSplitInfo.newBuilder()
          .setInputPath(path)
          .setInputSplit(inputSplits[idx])
          .setInputFormatClassName(inputFormatClassName)
          .build();
    }

    LOG.log(Level.FINE, "The number of splits: {0}", hdfsSplitInfos.length);
    return hdfsSplitInfos;
  }
}
