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
package edu.snu.cay.services.em.driver.api;

import org.apache.reef.driver.context.ActiveContext;
import org.apache.reef.driver.evaluator.AllocatedEvaluator;
import org.apache.reef.util.BuilderUtils;
import org.apache.reef.wake.EventHandler;

import java.util.List;

/**
 * Resource specification for EM.
 */
public final class EMResourceSpec {
  private String tableId = null;
  private Integer number = null;
  private Integer megaBytes = null;
  private Integer cores = null;
  private EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler = null;
  private List<EventHandler<ActiveContext>> contextActiveHandlerList = null;

  private EMResourceSpec() {
  }

  public String getTableId() {
    return tableId;
  }

  public int getNumber() {
    return number;
  }

  public int getMegaBytes() {
    return megaBytes;
  }

  public int getCores() {
    return cores;
  }

  public EventHandler<AllocatedEvaluator> getEvaluatorAllocatedHandler() {
    return evaluatorAllocatedHandler;
  }

  public List<EventHandler<ActiveContext>> getContextActiveHandlerList() {
    return contextActiveHandlerList;
  }

  /**
   * Create a new builder object.
   */
  public static Builder newBuilder() {
    return new EMResourceSpec.Builder();
  }

  public static final class Builder implements org.apache.reef.util.Builder<EMResourceSpec> {
    private final EMResourceSpec spec;
    private Builder() {
      spec = new EMResourceSpec();
    }

    /**
     * @param tableId identifier of the table which evaluators belong to
     */
    public Builder setTableId(final String tableId) {
      spec.tableId = tableId;
      return this;
    }

    /**
     * @param number number of evaluators to add
     */
    public Builder setNumber(final int number) {
      spec.number = number;
      return this;
    }

    /**
     * @param megaBytes memory size of each new evaluator in MB
     */
    public Builder setMegaBytes(final int megaBytes) {
      spec.megaBytes = megaBytes;
      return this;
    }

    /**
     * @param cores number of cores of each new evaluator
     */
    public Builder setCores(final int cores) {
      spec.cores = cores;
      return this;
    }

    /**
     * @param evaluatorAllocatedHandler callback which handles {@link AllocatedEvaluator} event
     */
    public Builder setEvaluatorAllocatedHandler(final EventHandler<AllocatedEvaluator> evaluatorAllocatedHandler) {
      spec.evaluatorAllocatedHandler = evaluatorAllocatedHandler;
      return this;
    }

    /**
     * @param contextActiveHandlerList callbacks which handle {@link ActiveContext} events, executed in sequence
     */
    public Builder setContextActiveHandlerList(final List<EventHandler<ActiveContext>> contextActiveHandlerList) {
      spec.contextActiveHandlerList = contextActiveHandlerList;
      return this;
    }

    @Override
    public EMResourceSpec build() {
      BuilderUtils.notNull(spec.number);
      BuilderUtils.notNull(spec.megaBytes);
      BuilderUtils.notNull(spec.cores);
      BuilderUtils.notNull(spec.evaluatorAllocatedHandler);
      BuilderUtils.notNull(spec.contextActiveHandlerList);
      return spec;
    }
  }
}
