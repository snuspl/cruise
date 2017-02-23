/*
 * Copyright (C) 2017 Seoul National University
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
package edu.snu.cay.common.datastorer;

import java.io.IOException;

/**
 * DataStorer service. Users can write serialized data to file system.
 * The path is determined by {@link edu.snu.cay.common.datastorer.param.BaseDir} and sub-path specified by users.
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
