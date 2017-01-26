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
package edu.snu.cay.services.et.driver.api;

import edu.snu.cay.common.dataloader.HdfsSplitInfo;
import edu.snu.cay.services.et.configuration.TableConfiguration;
import org.apache.reef.annotations.audience.DriverSide;

import java.util.List;
import java.util.Optional;

/**
 * Interface for master to send messages to executors.
 */
@DriverSide
public interface MessageSender {

  /**
   * Sends a TableInitMsg that initializes {@link edu.snu.cay.services.et.evaluator.api.Table} in an executor.
   * It includes metadata for initializing a table.
   * If {@code fileSplit} is present, the executor will load file into initialized local blocks.
   */
  void sendTableInitMsg(String executorId,
                        TableConfiguration tableConf,
                        List<String> blockOwnerList, int revision,
                        Optional<HdfsSplitInfo> fileSplit);

  /**
   * Sends a OwnershipUpdateMsg that notifies ownership update in other executors.
   */
  void sendOwnershipUpdateMsg(String executorId,
                              String tableId, int blockId,
                              String newOwnerId,
                              int revision);

  /**
   * Sends a MoveInitMsg that initializes migration process between two executors.
   */
  void sendMoveInitMsg(String tableId, List<Integer> blockIds,
                       String senderId, String receiverId);
}
