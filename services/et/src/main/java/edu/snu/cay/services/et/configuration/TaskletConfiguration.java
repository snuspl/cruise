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
package edu.snu.cay.services.et.configuration;

import edu.snu.cay.services.et.configuration.parameters.*;
import edu.snu.cay.services.et.evaluator.api.*;
import edu.snu.cay.services.et.evaluator.impl.*;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Configurations;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.BuilderUtils;

/**
 * A configuration required for submitting a Tasklet.
 */
public final class TaskletConfiguration {
  private final String id; // should be unique within the same executor
  private final Class<? extends Tasklet> taskletClass;
  private final Class<? extends TaskletCustomMsgHandler> taskletMsgHandlerClass;
  private final Configuration userParamConf;

  private Configuration configuration = null;

  private TaskletConfiguration(final String id,
                               final Class<? extends Tasklet> taskletClass,
                               final Class<? extends TaskletCustomMsgHandler> taskletMsgHandlerClass,
                               final Configuration userParamConf) {
    this.id = id;
    this.taskletClass = taskletClass;
    this.taskletMsgHandlerClass = taskletMsgHandlerClass;
    this.userParamConf = userParamConf;
  }

  /**
   * @return a table identifier
   */
  public String getId() {
    return id;
  }

  /**
   * @return a tasklet class
   */
  public Class<? extends Tasklet> getTaskletClass() {
    return taskletClass;
  }

  /**
   * @return a TaskletCustomMsgHandler implementation
   */
  public Class<? extends TaskletCustomMsgHandler> getTaskletMsgHandlerClass() {
    return taskletMsgHandlerClass;
  }

  /**
   * @return a user parameter configuration
   */
  public Configuration getUserParamConf() {
    return userParamConf;
  }

  /**
   * @return a tang {@link Configuration} that includes all metadata of table
   */
  public Configuration getConfiguration() {
    if (configuration == null) {
      configuration = Configurations.merge(
          Tang.Factory.getTang().newConfigurationBuilder()
              .bindNamedParameter(TaskletIdentifier.class, id)
              .bindImplementation(Tasklet.class, taskletClass)
              .bindImplementation(TaskletCustomMsgHandler.class, taskletMsgHandlerClass)
              .build(),
          userParamConf);
    }
    return configuration;
  }

  /**
   * @return a builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder of TableConfiguration.
   */
  public static final class Builder implements org.apache.reef.util.Builder<TaskletConfiguration> {
    /**
     * Required parameters.
     */
    private String id;
    private Class<? extends Tasklet> taskletClass;

    /**
     * Optional parameters.
     */
    private Class<? extends TaskletCustomMsgHandler> taskletMsgHandlerClass = DefaultTaskletCustomMsgHandler.class;
    private Configuration userParamConf = Tang.Factory.getTang().newConfigurationBuilder().build(); // empty conf

    private Builder() {
    }

    public Builder setId(final String id) {
      this.id = id;
      return this;
    }

    public Builder setTaskletClass(final Class<? extends Tasklet> taskletClass) {
      this.taskletClass = taskletClass;
      return this;
    }

    public Builder setTaskletMsgHandlerClass(final Class<? extends TaskletCustomMsgHandler> taskletMsgHandlerClass) {
      this.taskletMsgHandlerClass = taskletMsgHandlerClass;
      return this;
    }

    public Builder setUserParamConf(final Configuration userParamConf) {
      this.userParamConf = userParamConf;
      return this;
    }

    @Override
    public TaskletConfiguration build() {
      BuilderUtils.notNull(id);
      BuilderUtils.notNull(taskletClass);

      return new TaskletConfiguration(id, taskletClass, taskletMsgHandlerClass, userParamConf);
    }
  }
}
