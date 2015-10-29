/*
 * Copyright (C) 2015 Seoul National University
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
package edu.snu.cay.dolphin.examples.matmul;

import edu.snu.cay.dolphin.core.UserControllerTask;
import edu.snu.cay.dolphin.groupcomm.interfaces.DataReduceReceiver;
import org.apache.reef.io.data.output.OutputStreamProvider;

import javax.inject.Inject;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.*;

public final class MatrixMulCtrlTask extends UserControllerTask implements DataReduceReceiver<List<IndexedVector>> {

  private final OutputStreamProvider outputStreamProvider;

  @Inject
  private MatrixMulCtrlTask(final OutputStreamProvider outputStreamProvider) {
    this.outputStreamProvider = outputStreamProvider;
  }

  @Override
  public void run(final int iteration) {
    // no-op
  }

  @Override
  public boolean isTerminated(final int iteration) {
    return iteration > 0;
  }

  @Override
  public void receiveReduceData(final int iteration, final List<IndexedVector> data) {
    Collections.sort(data, new Comparator<IndexedVector>() {
      @Override
      public int compare(final IndexedVector o1, final IndexedVector o2) {
        return o1.getIndex() - o2.getIndex();
      }
    });

    try (final DataOutputStream daos = outputStreamProvider.create("result")) {
      if (data.size() == 0) {
        daos.writeBytes(String.format("No data"));
        return;
      }

      final int numColumns = data.size();
      final int numRows = data.get(0).getVector().size();
      for (int i = 0; i < numRows; i++) {
        daos.writeBytes(String.format("%d", i));
        for (int j = 0; j < numColumns; j++) {
          daos.writeBytes(String.format(" "));
          daos.writeBytes(String.format("%f", data.get(j).getVector().get(i)));
        }
        daos.writeBytes(String.format("%n"));
      }
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }
}
