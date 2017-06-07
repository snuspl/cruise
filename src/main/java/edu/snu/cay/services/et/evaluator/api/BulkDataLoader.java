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
package edu.snu.cay.services.et.evaluator.api;

import edu.snu.cay.services.et.evaluator.impl.NoneKeyBulkDataLoader;
import edu.snu.cay.services.et.exceptions.KeyGenerationException;
import edu.snu.cay.services.et.exceptions.TableNotExistException;
import org.apache.reef.tang.annotations.DefaultImplementation;

import java.io.IOException;

/**
 * An interface that loads data into table from a file.
 */
@DefaultImplementation(NoneKeyBulkDataLoader.class)
public interface BulkDataLoader {

  /**
   * Loads a data for table from a file specified by {@code serializedHdfsSplitInfo}.
   * @param serializedHdfsSplitInfo a serialized hdfs split info
   * @throws IOException when fail to create HdfsDataSet from {@code serializedSplitInfo}
   * @throws KeyGenerationException when fail to generate keys for data without key
   * @throws TableNotExistException when there's no initialized table with {@code tableId}
   */
  void load(String tableId, String serializedHdfsSplitInfo)
      throws IOException, KeyGenerationException, TableNotExistException;
}
