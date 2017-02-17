package edu.snu.cay.common.datastorer;

/**
 * Created by yunseong on 2/13/17.
 */
public interface DataStorer {
  void storeData(String path, byte[] data);
}
