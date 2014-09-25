package org.apache.reef.inmemory.driver;

import org.apache.hadoop.fs.Path;
import org.apache.reef.inmemory.common.entity.FileMeta;

import java.io.IOException;

public interface CacheUpdater {
  FileMeta updateMeta(Path path, FileMeta fileMeta) throws IOException;
}
