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
package edu.snu.cay.services.em.driver.api;

import java.util.List;

/**
 * An interface representing the update in EM's routing table.
 */
public interface EMRoutingTableUpdate {

  /**
   * Returns the store id that was the owner of updated blocks.
   * @return an old store id of blocks
   */
  int getOldOwnerId();

  /**
   * Returns the store id that is the owner of updated blocks.
   * @return a new store id of blocks
   */
  int getNewOwnerId();

  /**
   * Returns the ids of blocks that their locations are updated.
   * @return a list of block id
   */
  List<Integer> getBlockIds();
}
