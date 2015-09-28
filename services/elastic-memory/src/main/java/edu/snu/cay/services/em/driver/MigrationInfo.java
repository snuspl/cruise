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

import org.apache.commons.lang.math.LongRange;

import java.util.Collection;

/**
 * Encapsulates the status of a migration. It consists of the information of
 * the sender, receiver, data type, and range information. Each migration has
 * its state as described in {@link MigrationManager}.
 * The range information is set empty initially,
 * and updated when the data is transferred to the receiver (may be changed in #96)
 */
final class MigrationInfo {
  /**
   * Represents the status of the migration.
   */
  public enum State {
    SENDING_DATA, WAITING_UPDATE, UPDATING_RECEIVER, RECEIVER_UPDATED, MOVING_PARTITION, UPDATING_SENDER, FINISHED
  }

  private final String senderId;
  private final String receiverId;
  private final String dataType;
  private Collection<LongRange> ranges; // Ranges are updated once the driver receives the update message.
  private State state = State.SENDING_DATA;

  /**
   * Creates a new MigrationInfo when move() is requested.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param dataType Type of data
   */
  public MigrationInfo(final String senderId,
                       final String receiverId,
                       final String dataType) {
    this.senderId = senderId;
    this.receiverId = receiverId;
    this.dataType = dataType;
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
   * Update the data ranges.
   * @param ranges ranges of the data that participates in this migration.
   */
  public void setRanges(final Collection<LongRange> ranges) {
    this.ranges = ranges;
  }

  /**
   * @return The status of the migration.
   */
  public State getState() {
    return state;
  }

  /**
   * Update the status of the migration.
   * @param state Target state
   */
  public void setState(final State state) {
    this.state = state;
  }
}
