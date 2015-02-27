/**
 * Copyright (C) 2014 Seoul National University
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
package edu.snu.reef.flexion.examples.ml.sub;

import com.microsoft.reef.io.network.group.operators.Reduce;
import edu.snu.reef.flexion.examples.ml.data.VectorSum;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

/**
 * A reduce function that receives several maps and computes one final map.
 * If vector sums of same ids are present, this function adds them all and computes
 * a single vector sum for each id.
 */
public final class MapOfIntVSumReduceFunction implements Reduce.ReduceFunction<Map<Integer, VectorSum>> {

  @Inject
  public MapOfIntVSumReduceFunction() {
  }

  @Override
  public Map<Integer, VectorSum> apply(Iterable<Map<Integer, VectorSum>> elements) {
    final Map<Integer, VectorSum> resultMap = new HashMap<>();

    for (final Map<Integer, VectorSum> map : elements) {
      for (final Integer integer : map.keySet()) {
        if (resultMap.containsKey(integer)) {
          resultMap.get(integer).add(map.get(integer));
        } else {
          resultMap.put(integer, map.get(integer));
        }
      }
    }

    return resultMap;
  }
}
