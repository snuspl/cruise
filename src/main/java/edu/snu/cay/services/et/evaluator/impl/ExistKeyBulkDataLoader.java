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
import edu.snu.cay.services.et.evaluator.api.BulkDataLoader;
import edu.snu.cay.services.et.evaluator.api.DataParser;
import edu.snu.cay.services.et.evaluator.api.Table;
import edu.snu.cay.services.et.exceptions.KeyGenerationException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import edu.snu.cay.utils.MemoryUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;
import org.apache.reef.tang.InjectionFuture;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A class that loads key-value data for table blocks from a file.
 * Note that {@link DataParser} must has generic type {@link Pair<K, V>}.
 */
public final class ExistKeyBulkDataLoader<K, V> implements BulkDataLoader {

  private static final Logger LOG = Logger.getLogger(ExistKeyBulkDataLoader.class.getName());
  private final DataParser<Pair<K, V>> dataParser;
  private final InjectionFuture<Tables> tablesFuture;

  @Inject
  private ExistKeyBulkDataLoader(final DataParser<Pair<K, V>> dataParser,
                                 final InjectionFuture<Tables> tablesFuture) {
    this.dataParser = dataParser;
    this.tablesFuture = tablesFuture;
  }

  @Override
  public void load(final String tableId, final String serializedHdfsSplitInfo)
      throws IOException, KeyGenerationException, TableNotExistException {
    LOG.log(Level.INFO, "Before loading data. Used memory {0} MB", MemoryUtils.getUsedMemoryMB());
    final HdfsDataSet<?, Text> hdfsDataSet = HdfsDataSet.from(serializedHdfsSplitInfo);
    final List<String> rawDataList = new LinkedList<>();
    hdfsDataSet.forEach(pair -> rawDataList.add(pair.getValue().toString()));

    final List<Pair<K, V>> dataList = dataParser.parse(rawDataList);
    LOG.log(Level.INFO, "{0} data items have been loaded from hdfs. Used memory: {1} MB",
        new Object[] {dataList.size(), MemoryUtils.getUsedMemoryMB()});

    final Table<K, V, ?> loadTable = tablesFuture.get().getTable(tableId);
    try {
      loadTable.multiPut(dataList).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new RuntimeException(e);
    }
  }
}
