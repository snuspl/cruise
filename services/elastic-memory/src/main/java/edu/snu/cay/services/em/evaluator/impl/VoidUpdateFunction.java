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
package edu.snu.cay.services.em.evaluator.impl;

import edu.snu.cay.services.em.evaluator.api.EMUpdateFunction;
import org.apache.commons.lang3.NotImplementedException;

/**
 * An empty implementation of EMUpdateFunction for users who don't use update API of EM.
 */
public class VoidUpdateFunction implements EMUpdateFunction {

  private static final String NOT_IMPLEMENTED = "The implementation of EMUpdateFunction is not specified";

  @Override
  public Object getInitValue(final Object key) {
    throw new NotImplementedException(NOT_IMPLEMENTED);
  }

  @Override
  public Object getUpdateValue(final Object oldValue, final Object deltaValue) {
    throw new NotImplementedException(NOT_IMPLEMENTED);
  }
}
