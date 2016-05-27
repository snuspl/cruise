/*
 * Copyright (C) 2016 Seoul National University
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
package edu.snu.cay.dolphin.async.optimizer;

import org.apache.reef.io.data.loading.api.DataSet;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Iterator;

/**
 * An implementation of DataSet (in REEF DataLoading API), which does not contain any data. When the App uses
 * DataLoading Service, then Tasks can be initiated only when the data block to load is configured. However,
 * Tasks that are added by EM.add() do not have to load the data. This class allows Task initialization by injecting
 * DataSet without loading actual data.
 */
public final class EmptyDataSet implements DataSet {
  @Inject
  private EmptyDataSet() {
  }

  @Override
  public Iterator iterator() {
    return Collections.emptyIterator();
  }
}
