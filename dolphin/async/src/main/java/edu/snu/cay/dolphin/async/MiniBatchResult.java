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
 * Encapsulates the result of a mini-batch.
 */
public final class MiniBatchResult {
  public static final MiniBatchResult EMPTY_RESULT = new MiniBatchResult(Collections.emptyMap(), 0D, 0D, 0D, 0D, 0D);

  private final Map<CharSequence, Double> appMetrics;
  private final double computeTime;
  private final double totalPullTime;
  private final double totalPushTime;
  private final double avgPullTime;
  private final double avgPushTime;

  private MiniBatchResult(final Map<CharSequence, Double> appMetrics,
                          final double computeTime,
                          final double totalPullTime,
                          final double totalPushTime,
                          final double avgPullTime,
                          final double avgPushTime) {
    this.appMetrics = appMetrics;
    this.computeTime = computeTime;
    this.totalPullTime = totalPullTime;
    this.totalPushTime = totalPushTime;
    this.avgPullTime = avgPullTime;
    this.avgPushTime = avgPushTime;
  }

  /**
   * @return A builder to create an MiniBatchResult object.
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  public Map<CharSequence, Double> getAppMetrics() {
    return appMetrics;
  }

  public double getComputeTime() {
    return computeTime;
  }

  public double getTotalPullTime() {
    return totalPullTime;
  }

  public double getTotalPushTime() {
    return totalPushTime;
  }

  public double getAvgPullTime() {
    return avgPullTime;
  }

  public double getAvgPushTime() {
    return avgPushTime;
  }

  public static final class Builder implements org.apache.reef.util.Builder<MiniBatchResult> {
    private final Map<CharSequence, Double> appMetrics = new HashMap<>();
    private Double computeTime;
    private Double totalPullTime;
    private Double totalPushTime;
    private Double avgPullTime;
    private Double avgPushTime;

    @Override
    public MiniBatchResult build() {
      return new MiniBatchResult(appMetrics, computeTime, totalPullTime, totalPushTime, avgPullTime, avgPushTime);
    }

    public Builder setAppMetric(final String key, final double value) {
      appMetrics.put(key, value);
      return this;
    }

    public Builder setComputeTime(final Double computeTime) {
      this.computeTime = computeTime;
      return this;
    }

    public Builder setTotalPullTime(final Double totalPullTime) {
      this.totalPullTime = totalPullTime;
      return this;
    }

    public Builder setTotalPushTime(final Double totalPushTime) {
      this.totalPushTime = totalPushTime;
      return this;
    }

    public Builder setAvgPullTime(final Double avgPullTime) {
      this.avgPullTime = avgPullTime;
      return this;
    }

    public Builder setAvgPushTime(final Double avgPushTime) {
      this.avgPushTime = avgPushTime;
      return this;
    }
  }
}
