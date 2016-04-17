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

import edu.snu.cay.utils.StateMachine;
import org.apache.commons.lang.math.LongRange;

import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Encapsulates the status of a migration. It consists of the sender, receiver, data type, and range information.
 * Note that the range information is updated later because the exact range cannot be determined
 * before checking the Evaluator' range state.
 * Each migration has its states as described in {@link MigrationManager}.
 */
final class Migration {
  private static final Logger LOG = Logger.getLogger(Migration.class.getName());

  static final String SENDING_DATA = "SENDING_DATA";
  static final String WAITING_UPDATE = "WAITING_UPDATE";
  static final String UPDATING_RECEIVER = "UPDATING_RECEIVER";
  static final String MOVING_PARTITION = "MOVING_PARTITION";
  static final String UPDATING_SENDER = "UPDATING_SENDER";
  static final String FINISHED = "FINISHED";

  private final StateMachine stateMachine = initStateMachine();

  private final String senderId;
  private final String receiverId;
  private final String dataType;

  /**
   * The set of moved ranges, which is set after data is transferred.
   * All ranges are dense because Evaluators convert the ranges before sending DataAckMsg.
   */
  private Collection<LongRange> movedRanges;

  /**
   * Ids of blocks to move.
   */
  private final List<Integer> blockIds;

  private final Set<Integer> movedBlockIds;

  /**
   * Creates a new Migration when move() is requested.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of data.
   */
  @Deprecated
  public Migration(final String senderId,
                   final String receiverId,
                   final String dataType) {
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.dataType = dataType;
    this.blockIds = new ArrayList<>(0);
    this.movedBlockIds = new HashSet<>(0);
  }

  /**
   * Creates a new Migration when move() is requested.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of data.
   */
  @Deprecated
  public Migration(final String senderId,
                   final String receiverId,
                   final String dataType,
                   final List<Integer> blockIds) {
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.dataType = dataType;
    this.blockIds = blockIds;
    this.movedBlockIds = new HashSet<>(blockIds.size());
  }

  private StateMachine initStateMachine() {
    return StateMachine.newBuilder()
        .addState(SENDING_DATA, "Wait for the data transfer finishes")
        .addState(WAITING_UPDATE, "Wait for the user to call applyUpdates()")
        .addState(UPDATING_RECEIVER, "Update the Receiver's MemoryStore")
        .addState(MOVING_PARTITION, "Update the Driver's partition status")
        .addState(UPDATING_SENDER, "Update the Sender's MemoryStore")
        .addState(FINISHED, "All the migration steps are finished")
        .setInitialState(SENDING_DATA)
        .addTransition(SENDING_DATA, WAITING_UPDATE,
            "When the data is sent to the receiver, the sender and receiver both wait until apply update is called")
        .addTransition(WAITING_UPDATE, UPDATING_RECEIVER,
            "When applyUpdate() is called, apply the result of migration to the receiver first")
        .addTransition(UPDATING_RECEIVER, MOVING_PARTITION,
            "When the Receiver updated its status, then update the status of partitions")
        .addTransition(MOVING_PARTITION, UPDATING_SENDER,
            "After partition is updated, then update the Sender")
        .addTransition(UPDATING_SENDER, FINISHED,
            "When the Sender also updated its status, then finish the migration.")
        .build();
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
   * @return Data type of the migration.
   */
  public String getDataType() {
    return dataType;
  }

  /**
   * @return Ranges of the migration.
   */
  public Collection<LongRange> getMovedRanges() {
    return movedRanges;
  }

  /**
   * Set the information of the ranges that were moved. If the evaluator has less data, the number of units
   * included in the ranges could be smaller than requested.
   * @param movedRanges Ranges that were actually moved.
   */
  public void setMovedRanges(final Collection<LongRange> movedRanges) {
    this.movedRanges = movedRanges;
  }

  /**
   * Check whether the current state is in the {@code expectedCurrentState}.
   */
  void checkState(final String expectedCurrentState) {
    stateMachine.checkState(expectedCurrentState);
  }

  /**
   * Check the expected state, and move to the target state.
   */
  void checkAndUpdate(final String expectedCurrentState, final String target) {
    stateMachine.checkAndSetState(expectedCurrentState, target);
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
    if (!blockIds.contains(blockId) || movedBlockIds.contains(blockId)) {
      LOG.log(Level.WARNING, "Block id {0} seems to have finished already.", blockId);
    }

    movedBlockIds.add(blockId);
  }

  /**
   * @return True if all the blocks have moved successfully.
   */
  boolean isComplete() {
    // Fails early when the size is not equal.
    if (movedBlockIds.size() != blockIds.size()) {
      return false;
    }

    // return false if any of blocks are left.
    for (final int blockId : blockIds) {
      if (!movedBlockIds.contains(blockId)) {
        return false;
      }
    }
    return true;
  }
}
