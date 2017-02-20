package edu.snu.cay.common.datastorer;

import java.io.IOException;

/**
 * DataStorer service. Users can write serialized data to file system.
 * The path is determined by {@link edu.snu.cay.common.datastorer.param.BaseDir} and specified sub-path.
 */
public interface DataStorer {

  /**
   * Stores the serialized data to file system.
   * @param subPath sub-path of the file to store.
   * @param data serialized data
   * @throws IOException if failed while writing data
   */
  void storeData(String subPath, byte[] data) throws IOException;
}
