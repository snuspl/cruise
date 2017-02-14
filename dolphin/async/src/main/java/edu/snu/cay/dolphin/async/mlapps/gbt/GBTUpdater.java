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
import edu.snu.cay.common.math.linalg.VectorFactory;
import edu.snu.cay.services.ps.server.api.ParameterUpdater;
import org.apache.reef.tang.annotations.Parameter;

import javax.inject.Inject;

import java.util.LinkedList;
import java.util.List;

import static edu.snu.cay.dolphin.async.mlapps.gbt.GBTParameters.NumFeatures;

/**
 * {@link ParameterUpdater} for the GBTREEF application.
 * Simply subtract delta vectors to the old vectors stored in this server.
 * Vectors are initialized with 0 vector.
 */
final class GBTUpdater implements ParameterUpdater<Integer, List<Vector>, List<Vector>> {

  // If the data that is pushed to the server contains information of feature types, first vector of the list
  // is 0 vector.
  private static final int TYPE = 0;
  private final int numFeatures;
  private final VectorFactory vectorFactory;

  @Inject
  private GBTUpdater(@Parameter(NumFeatures.class) final int numFeatures,
                     final VectorFactory vectorFactory) {
    this.numFeatures = numFeatures;
    this.vectorFactory = vectorFactory;
  }

  @Override
  public List<Vector> process(final Integer key, final List<Vector> preValue) {
    return preValue;
  }

  @Override
  public List<Vector> update(final List<Vector> oldValue, final List<Vector> deltaValue) {
    if (deltaValue.get(0).get(0) == TYPE) {
      if (oldValue.size() == 0) {
        oldValue.add(vectorFactory.createDenseZeros(numFeatures + 1));
      }
      oldValue.get(0).addi(deltaValue.get(1));
    } else {
      oldValue.add(deltaValue.get(1));
    }
    return oldValue;
  }

  @Override
  public List<Vector> initValue(final Integer key) {
    return new LinkedList<>();
  }
}
