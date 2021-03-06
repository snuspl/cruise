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
package edu.snu.spl.cruise.ps.core.worker;

import edu.snu.spl.cruise.common.dataloader.*;
import edu.snu.spl.cruise.ps.CruisePSParameters;
import edu.snu.spl.cruise.services.et.evaluator.api.DataParser;
import org.apache.hadoop.io.Text;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;

/**
 * Provides test set for evaluating learned parameters with unobserved dataset.
 */
public final class TestDataProvider<T> {
  private final String testDataPath;
  private final DataParser<T> dataParser;

  @Inject
  private TestDataProvider(@Parameter(CruisePSParameters.TestDataPath.class) final String testDataPath,
                           final DataParser<T> dataParser) {
    this.testDataPath = testDataPath;
    this.dataParser = dataParser;
  }

  /**
   * Returns a list of test data items by reading and parsing a file.
   * @return a list of test data items
   * @throws IOException when fails to read test data
   */
  public List<T> getTestData() throws IOException {
    if (testDataPath.equals(CruisePSParameters.TestDataPath.NONE)) {
      return Collections.emptyList();
    }

    final HdfsSplitInfo[] splitInfos = HdfsSplitManager.getSplits(testDataPath, TextInputFormat.class.getName(), 1);
    final String serializedInfo = HdfsSplitInfoSerializer.serialize(splitInfos[0]);
    final HdfsDataSet<?, Text> hdfsDataSet = HdfsDataSet.from(serializedInfo);

    final List<String> rawDataList = new LinkedList<>();
    hdfsDataSet.forEach(pair -> rawDataList.add(pair.getValue().toString()));
    final List<T> dataList = dataParser.parse(rawDataList);
    return dataList;
  }
}
