package org.apache.reef.elastic.memory.task;

import org.apache.reef.annotations.audience.TaskSide;
import org.apache.reef.elastic.memory.Key;
import org.apache.reef.io.data.loading.api.DataSet;

import java.util.Collection;
import java.util.List;

/**
 * Task-side interface of ElasticMemoryService
 */
@TaskSide
interface ElasticMemoryServiceClient {

  /**
   * register data that must not be moved around
   * @param key key class that represents a certain data type
   * @param value data item to register
   * @param <T> the actual data type
   */
  public <T> void putLocal(Class<? extends Key<T>> key, T value);

  /**
   * register data that must not be moved around
   * @param key key class that represents a certain data type
   * @param values list of data items to register
   * @param <T> the actual data type
   */
  public <T> void putLocal(Class<? extends Key<T>> key, List<T> values);

  /**
   * register data that can be migrated around for job optimization to EM
   * @param key key class that represents a certain data type
   * @param value data item to register
   * @param <T> the actual data type
   */
  public <T> void putMovable(Class<? extends Key<T>> key, T value);

  /**
   * register data that can be migrated around for job optimization to EM
   * @param key key class that represents a certain data type
   * @param values list of data items to register
   * @param <T> the actual data type
   */
  public <T> void putMovable(Class<? extends Key<T>> key, List<T> values);

  /**
   * fetch data of a certain key from EM
   * @param key key class that represents a certain data type
   * @param <T> the actual data type
   * @return data corresponding to the input key
   */
  public <T> List<T> get(Class<? extends Key<T>> key);

  /**
   * Query driver-side about update status of elastic memory map
   */
  public boolean hasChanged();

}
