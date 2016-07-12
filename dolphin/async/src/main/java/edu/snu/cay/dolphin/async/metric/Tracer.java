/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async.metric;

/**
 * This class allows timestamps to be taken at different points of a job's progress.
 *
 * It traces the total time that must be taken into account as the job's execution time.
 * It also tracks the total number of measurements taken as well as the number of elements processed
 * during time intervals.
 */
public class Tracer {
  private long begin;
  private long sum = 0;
  private long count = 0;
  private long elemCount = 0;

  /**
   * Resets all measurements taken so far by this tracer.
   */
  public void resetTrace() {
    sum = 0;
    count = 0;
    elemCount = 0;
  }

  /**
   * Marks the start time of a timestamp measurement to be taken.
   */
  public void startTimer() {
    begin = System.currentTimeMillis();
  }

  /**
   * Calculates the time taken to process a certain number of elements
   * since the start time and adds this to the total time.
   *
   * @param processedElemCount the number of elements processed during the time interval
   */
  public void recordTime(final int processedElemCount) {
    sum += System.currentTimeMillis() - begin;
    ++count;
    elemCount += processedElemCount;
  }

  /**
   * Total amount of time taken for the execution during the "tracing".
   * @return total elapsed time in seconds.
   */
  public double totalElapsedTime() {
    return sum / 1000.0D;
  }

  /**
   * Average time taken per measurement for the "tracing".
   * @return avg time taken per measurement in seconds.
   */
  public double avgTimePerRecord() {
    if (count == 0) {
      return Double.POSITIVE_INFINITY;
    }

    return sum / count / 1000.0D;
  }

  /**
   * Average time taken per processed element for the "tracing".
   * @return avg time taken per element in seconds.
   */
  public double avgTimePerElem() {
    if (elemCount == 0) {
      return Double.POSITIVE_INFINITY;
    }

    return sum / elemCount / 1000.0D;
  }
}
