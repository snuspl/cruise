/**
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
package edu.snu.reef.flexion.examples.simple;

import edu.snu.reef.flexion.core.UserComputeTask;
import edu.snu.reef.flexion.groupcomm.interfaces.DataBroadcastReceiver;
import edu.snu.reef.flexion.groupcomm.interfaces.DataReduceSender;

import javax.inject.Inject;

public final class SimpleCmpTask extends UserComputeTask
    implements DataBroadcastReceiver<String>, DataReduceSender<Integer> {

  private String message = null;
  private Integer count = 0;

  @Inject
  private SimpleCmpTask() {
  }

  @Override
  public void run(int iteration) {
    System.out.println(message);
    count++;

  }

  @Override
  public void receiveBroadcastData(String data) {
    message = data;
    count = 0;
  }

  @Override
  public Integer sendReduceData(int iteration) {
    return count;
  }

}



