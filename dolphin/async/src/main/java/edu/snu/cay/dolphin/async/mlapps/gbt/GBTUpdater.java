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
import edu.snu.cay.services.ps.server.api.ParameterUpdater;

import javax.inject.Inject;

import java.util.LinkedList;
import java.util.List;

/**
 * {@link ParameterUpdater} for the GBTREEF application.
 * Forest(list of GBTrees in server) collects all the GBTrees that are built in runMiniBatch().
 * {@code process} function changes preValue's type from GBTree to the list of GBTree.
 * {@code update} function newGBTree to the forest.
 * {@code initValue} function initialize forest as an empty list.
 */
final class GBTUpdater implements ParameterUpdater<Integer, GBTree, List<GBTree>> {

  @Inject
  private GBTUpdater() {
  }

  /**
   * This method converts a preValue type(GBTree) into a value type(List<GBTree>).
   * To convert the type, add the preValue GBTree into the retValue list which is an empty list.
   */
  @Override
  public List<GBTree> process(final Integer key, final GBTree preValue) {
    final List<GBTree> retValue = new LinkedList<>();
    retValue.add(preValue);
    return retValue;
  }

  /**
   * This method adds newGBTree into a forest list.
   * newGBTree list's size is always one, because GBTree list with length one is created in the process method.
   */
  @Override
  public List<GBTree> update(final List<GBTree> forest, final List<GBTree> newGBTree) {
    assert (newGBTree.size() == 1);
    forest.add(newGBTree.get(0));
    return forest;
  }

  @Override
  public List<GBTree> initValue(final Integer key) {
    return new LinkedList<>();
  }
}
