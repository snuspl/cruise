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

import edu.snu.cay.services.et.configuration.metric.MetricServiceExecutorConf;
import org.apache.reef.tang.Configuration;
import org.apache.reef.tang.Tang;
import org.apache.reef.util.BuilderUtils;

/**
 * A configuration required for adding an executor.
 */
public final class ExecutorConfiguration {
  private final ResourceConfiguration resourceConf;
  private final MetricServiceExecutorConf metricServiceConf;
  private final Configuration userContextConf;
  private final Configuration userServiceConf;

  private ExecutorConfiguration(final ResourceConfiguration resourceConf,
                                final MetricServiceExecutorConf metricServiceConf,
                                final Configuration userContextConf,
                                final Configuration userServiceConf) {
    this.resourceConf = resourceConf;
    this.metricServiceConf = metricServiceConf;
    this.userContextConf = userContextConf;
    this.userServiceConf = userServiceConf;
  }

  public ResourceConfiguration getResourceConf() {
    return resourceConf;
  }

  public Configuration getMetricServiceConf() {
    return metricServiceConf.getConfiguration();
  }

  public Configuration getUserContextConf() {
    return userContextConf;
  }

  public Configuration getUserServiceConf() {
    return userServiceConf;
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * A builder of {@link ExecutorConfiguration}.
   */
  public static final class Builder implements org.apache.reef.util.Builder<ExecutorConfiguration> {
    /**
     * Required parameters.
     */
    private ResourceConfiguration resourceConf;

    /**
     * Optional parameters.
     */
    private MetricServiceExecutorConf metricServiceConf = MetricServiceExecutorConf.newBuilder().build(); // default
    private Configuration userContextConf = Tang.Factory.getTang().newConfigurationBuilder().build(); // empty conf
    private Configuration userServiceConf = Tang.Factory.getTang().newConfigurationBuilder().build(); // empty conf

    private Builder() {
    }

    /**
     * @param resourceConf resource configuration
     * @return this
     */
    public Builder setResourceConf(final ResourceConfiguration resourceConf) {
      this.resourceConf = resourceConf;
      return this;
    }

    /**
     * @param metricServiceExecutorConf configuration for customizing metric service.
     * @return this
     */
    public Builder setMetricServiceConf(final MetricServiceExecutorConf metricServiceExecutorConf) {
      this.metricServiceConf = metricServiceExecutorConf;
      return this;
    }

    /**
     * @param userContextConf a context configuration specified by user
     * @return this
     */
    public Builder setUserContextConf(final Configuration userContextConf) {
      this.userContextConf = userContextConf;
      return this;
    }

    /**
     * @param userServiceConf a service configuration specified by user
     * @return this
     */
    public Builder setUserServiceConf(final Configuration userServiceConf) {
      this.userServiceConf = userServiceConf;
      return this;
    }

    @Override
    public ExecutorConfiguration build() {
      BuilderUtils.notNull(resourceConf);

      return new ExecutorConfiguration(resourceConf, metricServiceConf, userContextConf, userServiceConf);
    }
  }
}
