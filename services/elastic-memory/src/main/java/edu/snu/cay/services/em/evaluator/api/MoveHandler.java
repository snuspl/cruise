package edu.snu.cay.services.em.evaluator.api;

import org.apache.reef.annotations.audience.EvaluatorSide;
import org.apache.reef.annotations.audience.Private;

import java.util.List;
import java.util.Map;

/**
 * Interface for updating the ownership and getting/putting data from/to the MemoryStore.
 * Methods in this class manage the ownership and data in the MemoryStore in block-level,
 * contrary to public APIs where the data is accessed in key-level.
 *
 */
@EvaluatorSide
@Private
interface MoveHandler<K> {
  /**
   * Called when the ownership arrives, to apply the change of ownership.
   * @param dataType the type of the data
   * @param blockId id of the block to update its owner
   * @param storeId id of the MemoryStore who will be the owner
   * @return True if the update is successful, false otherwise.
   */
  boolean updateOwnership(String dataType, int blockId, int storeId);

  /**
   * Sends the data in the blocks to another MemoryStore.
   * @param dataType the type of the data
   * @param blockId the identifier of block to send
   * @param data the data to put
   * @return True if the update is successful, false otherwise.
   */
  <V> boolean putData(String dataType, int blockId, Map<K, V> data);

  /**
   * Gets the data in the block.
   * @param dataType the type of the data
   * @param blockId id of the block to get
   * @return the data in the requested block.
   */
  Map<K, Object> getData(String dataType, int blockId);

  /**
   * Removes the data from the MemoryStore.
   * @param dataType the type of the data
   * @param blockId id of the block to remove
   * @return True if the data has been removed successfully, false otherwise.
   */
  boolean removeData(String dataType, int blockId);
}
