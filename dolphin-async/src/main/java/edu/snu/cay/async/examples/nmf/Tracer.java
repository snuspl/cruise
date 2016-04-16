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
package edu.snu.cay.async.examples.nmf;

/**
 * Class for tracing push, pull, and computation time.
 * Time unit used in this class is the second.
 */
public class Tracer {

  private final Item push;
  private final Item pull;
  private final Item compute;

  public Tracer() {
    this.push = new Item();
    this.pull = new Item();
    this.compute = new Item();
  }

  public void startPull() {
    pull.start();
  }

  public void finishPull(final int requestCount) {
    pull.end(requestCount);
  }

  public void startPush() {
    push.start();
  }

  public void finishPush(final int requestCount) {
    push.end(requestCount);
  }

  public void startCompute() {
    compute.start();
  }

  public void finishCompute(final int elemCount) {
    compute.end(elemCount);
  }

  public double getPushAvgTime() {
    return push.avg();
  }

  public double getPushAvgTimePerRequest() {
    return push.avgElement();
  }

  public double getPushSumTime() {
    return push.sum();
  }

  public double getPullAvgTime() {
    return pull.avg();
  }

  public double getPullAvgTimePerRequest() {
    return pull.avgElement();
  }

  public double getPullSumTime() {
    return pull.sum();
  }

  public double getComputeAvgTime() {
    return compute.avg();
  }

  public double getComputeAvgTimePerItem() {
    return compute.avgElement();
  }

  public double getComputeSumTime() {
    return compute.sum();
  }

  public void reset() {
    push.reset();
    pull.reset();
    compute.reset();
  }

  private static class Item {
    private long begin;
    private long sum = 0;
    private long count = 0;
    private long elemCount = 0;

    private void reset() {
      sum = 0;
      count = 0;
      elemCount = 0;
    }

    private void start() {
      begin = System.currentTimeMillis();
    }

    private void end(final int processedElemCount) {
      sum += System.currentTimeMillis() - begin;
      ++count;
      elemCount += processedElemCount;
    }

    private double sum() {
      return sum / 1000.0D;
    }

    private double avg() {
      return sum / count / 1000.0D;
    }

    private double avgElement() {
      return sum / elemCount / 1000.0D;
    }
  }
}
