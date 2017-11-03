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
package edu.snu.cay.services.et.driver.impl;

import java.util.List;

/**
 * Represents the result of a migration.
 */
public class MigrationResult {
  private final boolean isCompleted;
  private final String msg;
  private final List<Integer> migratedBlocks;

  public MigrationResult(final boolean isCompleted, final String msg, final List<Integer> migratedBlocks) {
    this.isCompleted = isCompleted;
    this.msg = msg;
    this.migratedBlocks = migratedBlocks;
  }

  public boolean isCompleted() {
    return isCompleted;
  }

  public String getMsg() {
    return msg;
  }

  public List<Integer> getMigratedBlocks() {
    return migratedBlocks;
  }
}
