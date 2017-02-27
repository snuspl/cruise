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
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.exceptions.KeyGenerationException;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that loads a file into a table.
 * It only supports tables with {@link Long} key and {@link String} values.
 * Note that this class is only for ordered tables.
 */
final class BulkDataLoader {
  private static final Logger LOG = Logger.getLogger(BulkDataLoader.class.getName());

  // utility classes should not be instantiated.
  private BulkDataLoader() {
  }

  /**
   * Load a file specified by {@code serializedHdfsSplitInfo} into the {@code table}.
   * To put them into local blocks, it utilizes a given {@link LocalKeyGenerator},
   * which produces keys that belong to local blocks.
   * @param table a table
   * @param serializedHdfsSplitInfo a serialized hdfs split info
   * @param localKeyGenerator a local key generator
   * @throws IOException when fail to create HdfsDataSet from {@code serializedSplitInfo}
   * @throws KeyGenerationException when fail to generate the enough number of keys
   */
  static void load(final Table<Long, String> table,
                   final String serializedHdfsSplitInfo,
                   final LocalKeyGenerator localKeyGenerator) throws IOException, KeyGenerationException {
    final HdfsDataSet<?, Text> hdfsDataSet = HdfsDataSet.from(serializedHdfsSplitInfo);
    final List<String> dataList = new LinkedList<>();
    hdfsDataSet.forEach(pair -> dataList.add(pair.getValue().toString()));
    final List<Long> keyList = localKeyGenerator.getKeys(dataList.size());

    LOG.log(Level.INFO, "Load {0} data items into a table", keyList.size());
    for (int idx = 0; idx < keyList.size(); idx++) {
      table.put(keyList.get(idx), dataList.get(idx));
    }
  }
}
