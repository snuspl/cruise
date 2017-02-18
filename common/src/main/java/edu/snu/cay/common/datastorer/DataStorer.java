package edu.snu.cay.common.datastorer;

import java.io.IOException;

/**
 *
 */
public interface DataStorer {
  void storeData(String subPath, byte[] data) throws IOException;
}
