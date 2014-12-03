package org.apache.reef.inmemory.driver.locality;

import org.apache.reef.inmemory.common.entity.FileMeta;

import javax.inject.Inject;

/**
 * Placeholder LocationSorter that returns the original FileMeta.
 * Use in local mode, where there is no need to sort locations.
 */
public final class LocalLocationSorter implements LocationSorter {
  @Inject
  public LocalLocationSorter() {
  }

  @Override
  public FileMeta sortMeta(FileMeta original, String clientHostname) {
    return original;
  }
}
