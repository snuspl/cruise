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

import edu.snu.cay.dolphin.async.mlapps.gbt.tree.GBTree;
import edu.snu.cay.services.et.evaluator.api.UpdateFunction;

import javax.inject.Inject;
import java.util.LinkedList;
import java.util.List;
import java.util.logging.Logger;

/**
 * Each vector with a word index represents a count vector whose elements are the
 * number of words in the corpus that are assigned to a specific topic. We use
 * numVocabs-th row as the total count vector for all word indices.
 * Note that the last (numTopics-th) element of the array represents number of non-zero elements in the array.
 */
public final class GBTETModelUpdateFunction implements UpdateFunction<Integer, List<GBTree>, List<GBTree>> {
  private static final Logger LOG = Logger.getLogger(GBTETModelUpdateFunction.class.getName());

  @Inject
  private GBTETModelUpdateFunction() {
  }

  @Override
  public List<GBTree> initValue(final Integer integer) {
    return new LinkedList<>();
  }

  @Override
  public List<GBTree> updateValue(final Integer integer, final List<GBTree> forest, final List<GBTree>  newGBTree) {
    assert (newGBTree.size() == 1);
    forest.add(newGBTree.get(0));
    return forest;
  }
}
