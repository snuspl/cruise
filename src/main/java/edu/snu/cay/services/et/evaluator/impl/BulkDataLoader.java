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
package edu.snu.cay.services.et.evaluator.impl;

import edu.snu.cay.common.dataloader.HdfsDataSet;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.exceptions.KeyGenerationException;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that loads data for table blocks from a file.
 * It only supports tables with {@link Long} key type.
 * Note that this class is only for ordered tables.
 */
final class BulkDataLoader<V> {
  private static final Logger LOG = Logger.getLogger(BulkDataLoader.class.getName());

  private final DataParser<V> dataParser;
  private final LocalKeyGenerator localKeyGenerator;

  @Inject
  private BulkDataLoader(final DataParser<V> dataParser,
                         final LocalKeyGenerator localKeyGenerator) {
    this.dataParser = dataParser;
    this.localKeyGenerator = localKeyGenerator;
  }

  /**
   * Loads a file specified by {@code serializedHdfsSplitInfo}.
   * It utilizes a {@link #localKeyGenerator} to obtain keys that belong to local blocks.
   * @param serializedHdfsSplitInfo a serialized hdfs split info
   * @throws IOException when fail to create HdfsDataSet from {@code serializedSplitInfo}
   * @throws KeyGenerationException when fail to generate the enough number of keys
   * @return a map between block id and a corresponding data map
   */
  @SuppressWarnings("unchecked")
  Map<Integer, Map<Long, V>> loadBlockData(final String serializedHdfsSplitInfo)
      throws IOException, KeyGenerationException {
    final HdfsDataSet<?, Text> hdfsDataSet = HdfsDataSet.from(serializedHdfsSplitInfo);

    final List<String> rawDataList = new LinkedList<>();
    hdfsDataSet.forEach(pair -> rawDataList.add(pair.getValue().toString()));
    final List<V> dataList = dataParser.parse(rawDataList);
    LOG.log(Level.INFO, "{0} data items have been loaded from Hdfs.", dataList.size());

    final Map<Integer, List<Long>> blockIdToKeyListMap = localKeyGenerator.getBlockToKeys(dataList.size());

    final Iterator<V> dataIterator = dataList.iterator();
    final Map<Integer, Map<Long, V>> blockIdToDataMap = new HashMap<>();
    blockIdToKeyListMap.forEach((blockId, keyList) -> {
      final Map<Long, V> dataMap = new HashMap<>();
      blockIdToDataMap.put(blockId, dataMap);
      keyList.forEach(key -> dataMap.put(key, dataIterator.next()));
    });
    LOG.log(Level.INFO, "Load {0} data items for {1} blocks.",
        new Object[]{dataList.size(), blockIdToDataMap.size()});

    return blockIdToDataMap;
  }
}
