/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.async.optimizer.impl;

import edu.snu.cay.dolphin.async.optimizer.api.DataInfo;
import edu.snu.cay.dolphin.async.optimizer.api.EvaluatorParameters;

import java.util.Map;

/**
 * A plain-old-data implementation of EvaluatorParameters.
 */
public final class EvaluatorParametersImpl implements EvaluatorParameters {
  private final String id;
  private final DataInfo dataInfo;
  private final Map<String, Double> metrics;

  public EvaluatorParametersImpl(final String id,
                                 final DataInfo dataInfo,
                                 final Map<String, Double> metrics) {
    this.id = id;
    this.dataInfo = dataInfo;
    this.metrics = metrics;
  }

  @Override
  public String getId() {
    return id;
  }

  @Override
  public DataInfo getDataInfo() {
    return dataInfo;
  }

  @Override
  public Map<String, Double> getMetrics() {
    return metrics;
  }
}
