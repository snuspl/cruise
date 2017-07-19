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
package edu.snu.cay.dolphin.async.mlapps.gbt;

import edu.snu.cay.common.dataloader.HdfsSplitFetcher;
import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.common.dataloader.HdfsSplitManager;
import edu.snu.cay.common.dataloader.TextInputFormat;
import edu.snu.cay.dolphin.async.DolphinParameters.*;
import edu.snu.cay.dolphin.async.mlapps.gbt.GBTTrainer.FeatureType;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;
import java.io.IOException;
import java.util.*;

/**
 * Parses metadata of GBTData.
 * Metadata should be in the following form :
 *
 * f_1:t_1 f_2:t_2 f_3:t_3 ... f_m:t_m
 *
 * Here, f_i is an index of the according feature and t_i is a type of the feature.
 * If t_i == 0, the feature's type is numerical type(CONTINUOUS).
 * If t_i == 1, the feature's type is categorical type(CATEGORICAL).
 */
final class GBTMetadataParser {
  private static final int NUM_SPLIT = 1;
  private static final Pair<Map<Integer, FeatureType>, Integer> NO_INFORMATION = Pair.of(Collections.emptyMap(), -1);

  private final int numFeatures;
  private final String metadataPath;

  @Inject
  private GBTMetadataParser(@Parameter(NumFeatures.class) final int numFeatures,
                            @Parameter(GBTParameters.MetadataPath.class) final String metadataPath) {
    this.numFeatures = numFeatures;
    this.metadataPath = metadataPath;
  }

  /**
   * @return Pair of map and integer. Map contains an information of each feature's type. Integer value is a number of
   *         y-value's possible types.
   */
  Pair<Map<Integer, FeatureType>, Integer> getFeatureTypes() {
    final HdfsSplitInfo[] infoArr =
        HdfsSplitManager.getSplits(metadataPath, TextInputFormat.class.getName(), NUM_SPLIT);

    // infoArr's length is always 1(NUM_SPLIT == 1).
    assert infoArr.length == 1;
    final HdfsSplitInfo info = infoArr[0];
    try {
      final Iterator<Pair<LongWritable, Text>> iterator = HdfsSplitFetcher.fetchData(info);
      while (iterator.hasNext()) {
        final String text = iterator.next().getValue().toString().trim();
        if (text.startsWith("#") || text.length() == 0) {
          // comments and empty lines
          continue;
        }
        
        final Map<Integer, FeatureType> featureTypes = new HashMap<>(numFeatures);
        final String[] splits = text.split(" ");
        for (final String split : splits) {
          final String[] idxVal = split.split(":");
          assert idxVal.length == 2;
          final int idx = Integer.parseInt(idxVal[0]);
          if (idx != numFeatures) {
            final FeatureType featureType =
                Integer.parseInt(idxVal[1]) == 0 ? FeatureType.CONTINUOUS : FeatureType.CATEGORICAL;
            featureTypes.put(idx, featureType);
          }
        }
        final int valueTypeNum = Integer.parseInt(splits[numFeatures].split(":")[1]);
        // All the necessary data should be in one line.
        return Pair.of(featureTypes, valueTypeNum);
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
    // If the file does not contain any information, return the pair of Collections.emptyMap() and -1.
    return NO_INFORMATION;
  }
}
