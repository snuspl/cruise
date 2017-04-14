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
package edu.snu.cay.services.et.evaluator.impl;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks the statistics about remote access ops.
 */
public final class RemoteAccessOpStat {

  private final Map<String, Integer> countsSentGetReq = new ConcurrentHashMap<>();
  private final Map<String, Long> bytesReceivedGetResp = new ConcurrentHashMap<>();

  @Inject
  private RemoteAccessOpStat() {
  }

  /**
   * Increments the count of Get requests that have been sent.
   * @param tableId a table id
   */
  public void incCountSentGetReq(final String tableId) {
    countsSentGetReq.merge(tableId, 0, (key, prevCount) -> prevCount + 1);
  }

  /**
   * Increments the number of bytes received by Get response.
   * @param tableId a table id
   * @param numBytes the number of bytes of Get responses
   */
  public void incBytesReceivedGetResp(final String tableId, final long numBytes) {
    bytesReceivedGetResp.merge(tableId, 0L, (key, prevNumBytes) -> prevNumBytes + numBytes);
  }

  /**
   * Return and clear the accumulated Get request counts for each table.
   * @return a map between table id and corresponding Get request counts
   */
  public Map<String, Integer> getCountsSentGetReq() {
    final Map<String, Integer> copied = new HashMap<>(countsSentGetReq);
    // may lose some increments happen between upper and below line
    countsSentGetReq.clear();
    return copied;
  }

  /**
   * Return and clear the accumulated Get response bytes for each table.
   * @return a map between table id and corresponding Get response bytes
   */
  public Map<String, Long> getBytesReceivedGetResp() {
    final Map<String, Long> copied = new HashMap<>(bytesReceivedGetResp);
    // may lose some increments happen between upper and below line
    bytesReceivedGetResp.clear();
    return copied;
  }
}
