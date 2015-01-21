package org.apache.reef.inmemory.driver;

import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.FileMeta;

import java.io.IOException;

/**
 * Used to update metadata, based on changes to the state on Tasks.
 * The metadata is updated to reflect removal logs:
 * @see org.apache.reef.inmemory.driver.CacheLocationRemover
 * After updating removals, if there are not enough copies of a block,
 * that block is then restored, from the Base FS.
 */
public interface CacheUpdater {
  /**
   * Update metadata in place, and then return a copy of the metadata.
   * The copy is made so it can be safely written on the network.
   * (Without a copy, the metadata could be concurrently updated at any point.)
   * Implementations must synchronize on the FileMeta to avoid concurrent updates.
   * @param path File path
   * @param fileMeta File metadata, which is updated in place
   * @return A copy of the update metadata
   * @throws IOException Indicates an error restoring data from the Base FS
   */
  FileMeta updateMeta(FileMeta fileMeta) throws IOException;
}
