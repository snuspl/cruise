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

import edu.snu.cay.common.math.linalg.Vector;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;

import javax.inject.Inject;

import java.util.LinkedList;
import java.util.List;

/**
 * {@link ParameterUpdater} for the GBTREEF application.
 * Forest list(in server) collects all the GBTrees that are built in runMiniBatch().
 * This updater's main role is adding newGBTree to the forest list.
 * Vectors are initialized with 0 vector.
 */
final class GBTUpdater implements ParameterUpdater<Integer, Vector, List<Vector>> {

  @Inject
  private GBTUpdater() {
  }

  /**
   * This method converts a preValue type(Vector) into a value type(List<Vector>).
   * To convert the type, add the preValue vector into the retValue list which is an empty vector list.
   */
  @Override
  public List<Vector> process(final Integer key, final Vector preValue) {
    final List<Vector> retValue = new LinkedList<>();
    retValue.add(preValue);
    return retValue;
  }

  /**
   * This method adds newGBTree into a forest list.
   * newGBTree list's size is always one, because vector list with length one is created in the process method.
   */
  @Override
  public List<Vector> update(final List<Vector> forest, final List<Vector> newGBTree) {
    assert (newGBTree.size() == 1);
    forest.add(newGBTree.get(0));
    return forest;
  }

  @Override
  public List<Vector> initValue(final Integer key) {
    return new LinkedList<>();
  }
}
