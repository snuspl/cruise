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
package edu.snu.cay.dolphin.async.optimizer.impl;

import edu.snu.cay.common.dataloader.HdfsSplitFetcher;
import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitManager;
import edu.snu.cay.common.dataloader.TextInputFormat;
import edu.snu.cay.common.param.Parameters;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.annotations.audience.Private;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

/**
 * Parses the core information from a file.
 */
@Private
public final class CoreInfoParser {
  private final String hostnameToCoreFilePath;
  
  @Inject
  private CoreInfoParser(@Parameter(Parameters.HostToCoreFilePath.class) final String hostCoreFilePath) {
    this.hostnameToCoreFilePath = hostCoreFilePath;
  }
  
  /**
   * @return the mapping between the hostname and bandwidth of machines
   */
  public Map<String, Integer> parseCoreInfo() {
    if (hostnameToCoreFilePath.equals(Parameters.HostToCoreFilePath.NONE)) {
      return Collections.emptyMap();
    }
    
    final Map<String, Integer> mapping = new HashMap<>();
    
    final HdfsSplitInfo[] infoArr =
        HdfsSplitManager.getSplits(hostnameToCoreFilePath, TextInputFormat.class.getName(), 1);
    
    assert infoArr.length == 1; // infoArr's length is always 1(NUM_SPLIT == 1).
    final HdfsSplitInfo info = infoArr[0];
    try {
      final Iterator<Pair<LongWritable, Text>> iterator = HdfsSplitFetcher.fetchData(info);
      while (iterator.hasNext()) {
        final String text = iterator.next().getValue().toString().trim();
        if (!text.startsWith("#") && text.length() != 0) { // comments and empty lines
          final String[] split = text.split("\\s+");
          assert split.length == 2;
          final String hostname = split[0];
          final int coreNum = Integer.parseInt(split[1]);
          mapping.put(hostname, coreNum);
        }
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    
    return mapping;
  }
}
