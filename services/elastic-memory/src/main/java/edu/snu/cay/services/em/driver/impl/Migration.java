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
package edu.snu.cay.services.em.driver.impl;

import org.htrace.TraceScope;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Encapsulates the status of a migration.
 * It consists of the sender, receiver, and blocks to move.
 */
final class Migration {
  private static final Logger LOG = Logger.getLogger(Migration.class.getName());

  private final String senderId;
  private final String receiverId;

  /**
   * Ids of blocks to move.
   */
  private final List<Integer> blockIds;

  /**
   * Ids of blocks have been moved.
   */
  private final Set<Integer> movedBlockIds;

  private final TraceScope traceScope;

  /**
   * Creates a new Migration when move() is requested.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param blockIds Identifiers of the blocks to move.
   */
  public Migration(final String senderId,
                   final String receiverId,
                   final List<Integer> blockIds,
                   final TraceScope traceScope) {
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.blockIds = blockIds;
    this.movedBlockIds = new HashSet<>(blockIds.size());
    this.traceScope = traceScope;
  }

  /**
   * @return Sender's identifier of the migration.
   */
  public String getSenderId() {
    return senderId;
  }

  /**
   * @return Receiver's identifier of the migration.
   */
  public String getReceiverId() {
    return receiverId;
  }

  /**
   * @return ids of the blocks that move
   */
  List<Integer> getBlockIds() {
    return blockIds;
  }

  /**
   * Marks one block as moved.
   * @param blockId id of the block to mark as moved.
   */
  void markBlockAsMoved(final int blockId) {
    if (!blockIds.contains(blockId)) {
      LOG.log(Level.WARNING, "Block id {0} was not asked to move.", blockId);
    } else if (movedBlockIds.contains(blockId)) {
      LOG.log(Level.WARNING, "Block id {0} seems to have finished already.", blockId);
    } else {
      movedBlockIds.add(blockId);
    }
  }

  TraceScope getTraceScope() {
    return traceScope;
  }

  /**
   * @return True if all the blocks have moved successfully.
   */
  boolean isComplete() {
    return movedBlockIds.size() == blockIds.size();
  }
}
