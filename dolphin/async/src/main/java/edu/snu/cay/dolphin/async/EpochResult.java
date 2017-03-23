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
package edu.snu.cay.dolphin.async;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates the result of an epoch.
 */
public final class EpochResult {
  public static final EpochResult EMPTY_RESULT = new EpochResult(Collections.emptyMap());

  private final Map<CharSequence, Double> appMetrics;

  private EpochResult(final Map<CharSequence, Double> appMetrics) {
    this.appMetrics = appMetrics;
  }

  /**
   * @return A builder to create an EpochResult object.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public Map<CharSequence, Double> getAppMetrics() {
    return appMetrics;
  }

  public static final class Builder implements org.apache.reef.util.Builder<EpochResult> {
    private final Map<CharSequence, Double> appMetrics = new HashMap<>();

    @Override
    public EpochResult build() {
      return new EpochResult(appMetrics);
    }

    public Builder addAppMetric(final String key, final double value) {
      appMetrics.put(key, value);
      return this;
    }
  }
}
