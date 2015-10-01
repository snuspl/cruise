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
package edu.snu.cay.services.em.driver;

import edu.snu.cay.utils.StateMachine;
import org.apache.commons.lang.math.LongRange;

import java.util.Collection;

/**
 * Encapsulates the status of a migration. It consists of the information of
 * the sender, receiver, data type, and range information. Each migration has
 * its state as described in {@link MigrationManager}.
 */
final class Migration {
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
  private final Collection<LongRange> ranges;

  /**
   * Creates a new Migration when move() is requested.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of data
   */
  public Migration(final String senderId,
                   final String receiverId,
                   final String dataType,
                   final Collection<LongRange> ranges) {
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.dataType = dataType;
    this.ranges = ranges;
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
            // TODO #96: We need to remove the barrier.
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
   * Note that this field can be initially null and updated afterward.
   */
  public Collection<LongRange> getRanges() {
    return ranges;
  }

  /**
   * Check the expected state, and move to the target state.
   */
  void checkAndUpdate(final String expectedCurrentState, final String target) {
    stateMachine.checkAndSetState(expectedCurrentState, target);
  }
}
