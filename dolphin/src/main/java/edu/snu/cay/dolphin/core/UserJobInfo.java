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
package edu.snu.cay.dolphin.core;

import java.util.List;

/**
 * Interface for a user-defined job, which is a unit of work in Dolphin.
 * This class should be implemented by a user-defined job
 * which specify a data parser and stages composing the job
 */
public interface UserJobInfo {
  public abstract List<StageInfo> getStageInfoList();

  public abstract Class<? extends DataParser> getDataParser();
}
