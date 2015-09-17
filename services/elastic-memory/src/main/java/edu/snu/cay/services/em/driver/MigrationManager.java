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

import org.apache.reef.annotations.audience.DriverSide;

import javax.inject.Inject;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages ongoing Migrations.
 */
@DriverSide
// TODO #96: Let's revisit the thread-safety later.
public final class MigrationManager {
  /**
   * Represents the status of the migration.
   */
  private enum State {
      SENDING, UPDATING_RECEIVER, UPDATING_SENDER
  }

  private final ConcurrentHashMap<CharSequence, MigrationInfo> ongoingMigrations = new ConcurrentHashMap<>();

  @Inject
  private MigrationManager() {
  }

  /**
   * Registers a migration with the information.
   * @param operationId Identifier of the {@code move} operation.
   * @param senderId Identifier of the sender
   * @param receiverId Identifier of the receiver
   */
  public void put(final CharSequence operationId, final String senderId, final String receiverId) {
    ongoingMigrations.put(operationId, new MigrationInfo(senderId, receiverId));
  }

  /**
   * Called when the data has been sent to the receiver successfully.
   * @param operationId Identifier of {@code move} operation.
   */
  public void onDataSent(final CharSequence operationId) {
    final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
    assert migrationInfo.state == State.SENDING;
    migrationInfo.state = State.UPDATING_RECEIVER;
  }

  /**
   * Called when the MemoryStore's state has been updated in the receiver successfully.
   * @param operationId Identifier of {@code move} operation.
   */
  public void onReceiverUpdated(final CharSequence operationId) {
    final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
    assert migrationInfo.state == State.UPDATING_RECEIVER;
    migrationInfo.state = State.UPDATING_SENDER;
  }

  /**
   * Called when the MemoryStore's state has been updated in the sender successfully.
   * @param operationId Identifier of {@code move} operation.
   */
  public void onSenderUpdated(final CharSequence operationId) {
    final MigrationInfo migrationInfo = ongoingMigrations.get(operationId);
    assert migrationInfo.state == State.UPDATING_SENDER;
    ongoingMigrations.remove(operationId);
  }

  /**
   * @param operationId Identifier of {@code move} operation.
   * @return Identifier of whom sends the data.
   */
  public String getSenderId(final CharSequence operationId) {
    return ongoingMigrations.get(operationId).senderId;
  }

  /**
   * @param operationId Identifier of {@code move} operation.
   * @return Identifier of whom receives the data.
   */
  public String getReceiverId(final CharSequence operationId) {
    return ongoingMigrations.get(operationId).receiverId;
  }

  /**
   * Inner class that encapsulates a migration.
   */
  private final class MigrationInfo {
    private final String senderId;
    private final String receiverId;
    private State state;

    /**
     * Creates a new MigrationInfo when move() is requested.
     * @param senderId Identifier of the sender.
     * @param receiverId Identifier of the receiver.
     */
    MigrationInfo(final String senderId, final String receiverId) {
      this.senderId = senderId;
      this.receiverId = receiverId;
      this.state = State.SENDING;
    }
  }
}
