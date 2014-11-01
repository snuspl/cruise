package org.apache.reef.inmemory.driver.locality;

import org.apache.reef.inmemory.common.entity.FileMeta;

/**
 * Sort block locations by locality
 */
public interface LocationSorter {
  /**
   * Sort block locations within a FileMeta by locality based on the client
   * @param original FileMeta with unsorted block locations
   * @param clientHostname Client to base sorting on
   * @return A new FileMeta with sorted block locations
   */
  FileMeta sortMeta(FileMeta original, String clientHostname);
}
