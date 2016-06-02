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
package edu.snu.cay.dolphin.async.mlapps.nmf;

/**
 * Class for tracing elapsed time and calculating statistics.
 * Time unit used in this class is the second.
 */
public class Tracer {
  private long begin;
  private long sum = 0;
  private long count = 0;
  private long elemCount = 0;

  public void reset() {
    sum = 0;
    count = 0;
    elemCount = 0;
  }

  public void start() {
    begin = System.currentTimeMillis();
  }

  public void end(final int processedElemCount) {
    sum += System.currentTimeMillis() - begin;
    ++count;
    elemCount += processedElemCount;
  }

  public double sum() {
    return sum / 1000.0D;
  }

  public double avg() {
    return sum / count / 1000.0D;
  }

  public double avgElement() {
    return sum / elemCount / 1000.0D;
  }
}
