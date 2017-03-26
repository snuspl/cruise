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
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.exceptions.KeyGenerationException;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that loads a file into a table.
 * It only supports tables with {@link Long} key type.
 * Note that this class is only for ordered tables.
 */
final class BulkDataLoader<V> {
  private static final Logger LOG = Logger.getLogger(BulkDataLoader.class.getName());

  private final Table<Long, V, ?> table;
  private final DataParser<V> dataParser;
  private final LocalKeyGenerator localKeyGenerator;

  @Inject
  private BulkDataLoader(final Table<Long, V, ?> table,
                         final DataParser<V> dataParser,
                         final LocalKeyGenerator localKeyGenerator) {
    this.table = table;
    this.dataParser = dataParser;
    this.localKeyGenerator = localKeyGenerator;
  }

  /**
   * Load a file specified by {@code serializedHdfsSplitInfo} into the {@code table}.
   * To put them into local blocks, it utilizes a {@link #localKeyGenerator},
   * which produces keys that belong to local blocks.
   * @param serializedHdfsSplitInfo a serialized hdfs split info
   * @throws IOException when fail to create HdfsDataSet from {@code serializedSplitInfo}
   * @throws KeyGenerationException when fail to generate the enough number of keys
   */
  void load(final String serializedHdfsSplitInfo) throws IOException, KeyGenerationException {
    final HdfsDataSet<?, Text> hdfsDataSet = HdfsDataSet.from(serializedHdfsSplitInfo);

    final List<String> rawDataList = new LinkedList<>();
    hdfsDataSet.forEach(pair -> rawDataList.add(pair.getValue().toString()));
    final List<V> dataList = dataParser.parse(rawDataList);

    final List<Long> keyList = localKeyGenerator.getKeys(dataList.size());
    LOG.log(Level.INFO, "Load {0} data items into a table", keyList.size());
    for (int idx = 0; idx < keyList.size(); idx++) {
      table.put(keyList.get(idx), dataList.get(idx));
    }
  }
}
