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
package edu.snu.cay.services.et.driver.impl;

import java.util.*;

/**
 * Encapsulates the status of a migration.
 * It consists of the sender, receiver, and blocks to move.
 */
final class Migration {
  /**
   * Metadata of migration.
   */
  private final MigrationMetadata migrationMetadata;

  /**
   * Ids of blocks whose data has been migrated.
   */
  private final Set<Integer> movedBlockIds;

  private final BlockManager blockManager;

  /**
   * Creates a new Migration when move() is requested.
   * @param senderId Identifier of the sender.
   * @param receiverId Identifier of the receiver.
   * @param blockIds Identifiers of the blocks to move.
   */
  Migration(final String senderId,
            final String receiverId,
            final String tableId,
            final List<Integer> blockIds,
            final BlockManager blockManager) {
    this.migrationMetadata = new MigrationMetadata(senderId, receiverId, tableId, blockIds);
    this.movedBlockIds = Collections.synchronizedSet(new HashSet<>(blockIds.size()));
    this.blockManager = blockManager;
  }

  MigrationMetadata getMigrationMetadata() {
    return migrationMetadata;
  }

  BlockManager getBlockManager() {
    return blockManager;
  }

  /**
   * Marks a block as moved.
   * @param blockId id of the block to mark as moved.
   */
  void markBlockAsMoved(final int blockId) {
    if (!migrationMetadata.getBlockIds().contains(blockId)) {
      throw new RuntimeException(String.format("Block %d was not asked to move.", blockId));
    }

    synchronized (movedBlockIds) {
      if (movedBlockIds.contains(blockId)) {
        throw new RuntimeException(String.format("Block %d seems to have finished already.", blockId));
      } else {
        movedBlockIds.add(blockId);
      }
    }
  }

  List<Integer> getMovedBlocks() {
    return new ArrayList<>(movedBlockIds);
  }

  /**
   * @return True if all the blocks have moved successfully.
   */
  boolean isComplete() {
    return movedBlockIds.size() == migrationMetadata.getBlockIds().size();
  }

  /**
   * A migration metadata, which is immutable.
   */
  final class MigrationMetadata {
    private final String senderId;
    private final String receiverId;
    private final String tableId;

    /**
     * Ids of blocks to move.
     */
    private final List<Integer> blockIds;

    private MigrationMetadata(final String senderId, final String receiverId,
                      final String tableId, final List<Integer> blockIds) {
      this.senderId = senderId;
      this.receiverId = receiverId;
      this.tableId = tableId;
      this.blockIds = blockIds;
    }

    /**
     * @return Sender's identifier of the migration.
     */
    String getSenderId() {
      return senderId;
    }

    /**
     * @return Receiver's identifier of the migration.
     */
    String getReceiverId() {
      return receiverId;
    }

    /**
     * @return the table id
     */
    String getTableId() {
      return tableId;
    }

    /**
     * @return ids of the blocks that move
     */
    List<Integer> getBlockIds() {
      return blockIds;
    }
  }
}
