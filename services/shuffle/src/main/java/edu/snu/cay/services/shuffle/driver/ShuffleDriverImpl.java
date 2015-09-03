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
package edu.snu.cay.services.shuffle.driver;

import edu.snu.cay.services.shuffle.common.ShuffleDescription;
import edu.snu.cay.services.shuffle.params.ShuffleParameters;
import org.apache.reef.tang.Injector;
import org.apache.reef.tang.annotations.Parameter;
import org.apache.reef.tang.exceptions.InjectionException;

import javax.inject.Inject;
import java.util.HashMap;
import java.util.Map;

final class ShuffleDriverImpl implements ShuffleDriver {

  private final Class<? extends ShuffleManager> shuffleManagerClass;
  private final Injector rootInjector;

  private final Map<String, ShuffleManager> managerMap;

  /**
   * Construct a shuffle driver.
   *
   * @param shuffleManagerClassName a name of ShuffleManager class
   * @param rootInjector the root injector to share components that are already created
   */
  @Inject
  private ShuffleDriverImpl(
      @Parameter(ShuffleParameters.ShuffleManagerClassName.class) final String shuffleManagerClassName,
      final Injector rootInjector) {
    try {
      this.shuffleManagerClass = (Class<? extends ShuffleManager>) Class.forName(shuffleManagerClassName);
    } catch (final ClassNotFoundException e) {
      throw new RuntimeException(e);
    }

    this.rootInjector = rootInjector;
    this.managerMap = new HashMap<>();
  }

  @Override
  public synchronized <K extends ShuffleManager> K registerShuffle(final ShuffleDescription shuffleDescription) {
    if (managerMap.containsKey(shuffleDescription.getShuffleName())) {
      throw new RuntimeException(shuffleDescription.getShuffleName()
          + " was already registered in ShuffleDriver");
    }

    // fork the root injector to share common components that are already instantiated in the driver.
    final Injector injector = rootInjector.forkInjector();
    injector.bindVolatileInstance(ShuffleDescription.class, shuffleDescription);
    try {
      final K manager = (K)injector.getInstance(shuffleManagerClass);
      final String shuffleName = shuffleDescription.getShuffleName();
      managerMap.put(shuffleName, manager);
      return manager;
    } catch (final InjectionException e) {
      throw new RuntimeException(e);
    }
  }
}
